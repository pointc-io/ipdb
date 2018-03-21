package slice

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/pointc-io/sliced"
	"github.com/pointc-io/sliced/api"

	cmd "github.com/pointc-io/sliced/command"
	"github.com/pointc-io/sliced/index/btree"
	"github.com/pointc-io/sliced/item"
	"github.com/pointc-io/sliced/redcon"
	"github.com/pointc-io/sliced/service"
)

const (
	getName    = "GET"
	setName    = "SET"
	delName    = "DEL"
	txModeName = "TX"

	raftInstallSnapshotName = "RAFTINSTALL"
	raftAppendName          = "RAFTAPPEND"
	raftVoteName            = "RAFTVOTE"
	raftChunkName           = "CHUNK"
	raftDoneName            = "DONE"
	raftSnapshotName        = "RAFTSNAPSHOT"
	raftSnapshotsName       = "RAFTSNAPSHOTS"
	raftJoinName            = "RAFTJOIN"
	raftRemoveName          = "RAFTREMOVE"
	raftStatsName           = "RAFTSTATS"
	raftStateName           = "RAFTSTATE"
	raftLeaderName          = "RAFTLEADER"
	raftShrinkName          = "RAFTSHRINK"
)

var (
	raftInstall = []byte(raftInstallSnapshotName)
	raftAppend  = []byte(raftAppendName)
	raftVote    = []byte(raftVoteName)
	raftChunk   = []byte(raftChunkName)
	raftDone    = []byte(raftDoneName)
	raftJoin    = []byte(raftJoinName)
	raftStats   = []byte(raftStatsName)
	raftState   = []byte(raftStateName)
	raftLeader  = []byte(raftLeaderName)
	raftShrink  = []byte(raftShrinkName)
)

// SliceMaster is broken up into Partitions that are assigned to a slot.
// There are 16384 slots.
// Partitions are balanced first across cores then nodes.

type Slots struct {
	Index  [16384]*Slice
	Shards []*Slice
}

// SliceMaster maintains master wide consensus which maintains shard raft groups.
// Each shard needs a minimum of 3 nodes.
// Slice selection is determined using a CRC16 on the key and follows the same paradigm as Redis Cluster.
type SliceMaster struct {
	service.BaseService

	host   string
	path   string
	bind   string
	master *Slice
	slices []*Slice
	table  [16384]*Slice

	mu sync.RWMutex

	tree *btree.BTree
}

func NewMaster(host, path string) *SliceMaster {
	d := &SliceMaster{
		host:   host,
		path:   path,
		slices: make([]*Slice, 0, 8),
		tree:   btree.New(64, nil),
	}

	d.BaseService = *service.NewBaseService(sliced.Logger, "db", d)
	return d
}

func (m *SliceMaster) Master() api.Slice {
	return m.master
}

func (d *SliceMaster) SliceOf(key string) api.Slice {
	slot := redcon.Slot(key)
	d.mu.RLock()
	idx := slot % len(d.slices)
	slice := d.slices[idx]
	d.mu.RUnlock()
	return slice
}

func (d *SliceMaster) SliceFor(id int) api.Slice {
	var shard *Slice
	d.mu.RLock()
	if id == -1 {
		shard = d.master
		d.mu.RUnlock()
		return shard
	}
	if id < 0 || id >= len(d.slices) {
		d.mu.RUnlock()
		return nil
	}
	shard = d.slices[id]
	d.mu.RUnlock()
	return shard
}

func (d *SliceMaster) sliceForKey(key string) *Slice {
	slot := redcon.Slot(key)
	d.mu.RLock()
	idx := slot % len(d.slices)
	slice := d.slices[idx]
	d.mu.RUnlock()
	return slice
}

func (d *SliceMaster) SliceCount() int {
	d.mu.RLock()
	l := len(d.slices)
	d.mu.RUnlock()
	return l
}

func (d *SliceMaster) SliceID(key string) int {
	slot := redcon.Slot(key)
	d.mu.RLock()
	idx := slot % len(d.slices)
	d.mu.RUnlock()
	return idx
}

func (d *SliceMaster) SliceForSlot(id int) *Slice {
	var shard *Slice
	d.mu.RLock()
	if id == -1 {
		shard = d.master
		d.mu.RUnlock()
		return shard
	}
	if id < 0 || id >= len(d.slices) {
		d.mu.RUnlock()
		return nil
	}
	shard = d.slices[id]
	d.mu.RUnlock()
	return shard
}

func (d *SliceMaster) slicePath(id int) (string, error) {
	if d.path == ":memory:" {
		return d.path, nil
	} else {
		var path string
		if id == -1 {
			path = filepath.Join(d.path, "master")
		} else {
			path = filepath.Join(d.path, "slices", fmt.Sprintf("%d", id))
		}
		return path, os.MkdirAll(path, 0700)
	}
}

func (d *SliceMaster) OnStart() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	clusterPath, err := d.slicePath(-1)

	if err != nil {
		d.Logger.Fatal().Err(err).Msgf("failed to mkdirs for \"%s\"", clusterPath)
		return err
	}

	// Load master shard.
	d.master = NewSlice(-1, d.path == ":memory:", clusterPath, d.host, d)
	err = d.master.Start()
	if err != nil {
		d.Logger.Error().Err(err).Msg("master db start failed")
		return err
	}

	if d.master.set.Length() == 0 {
		for i := 0; i < 1; i++ {
			shardPath, err := d.slicePath(i)

			if err != nil {
				d.Logger.Fatal().Err(err).Msgf("failed to mkdirs for \"%s\"", clusterPath)
				return err
			}

			// Create the first shard.
			shard := NewSlice(i, d.path == ":memory:", shardPath, d.host, d)
			d.slices = append(d.slices, shard)
		}
	}

	for i := 0; i < len(d.slices); i++ {
		err := d.slices[i].Start()

		if err != nil {
			for a := 0; a < i; a++ {
				d.slices[a].Stop()
			}
			d.Logger.Error().Err(err).Msgf("failed to start shard %d", i)
			return err
		}
	}
	return nil
}

func (d *SliceMaster) OnStop() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i := 0; i < len(d.slices); i++ {
		err := d.slices[i].Stop()

		if err != nil {
			d.Logger.Error().Err(err)
		} else {
			d.slices[i].Wait()
		}
	}
}

func (d *SliceMaster) Commit(ctx *cmd.Context) {
	if len(ctx.Changes) == 0 {
		return
	}

	//LOOP:
	for key, set := range ctx.Changes {
		slice := d.SliceForSlot(key)
		if slice == nil {
			for i := 0; i < len(set.Cmds); i++ {
				ctx.Out = redcon.AppendError(ctx.Out, fmt.Sprintf("ERR slice %d not on node", key))
			}
		} else {
			slice.Commit(ctx, set)
			ctx.Set = nil

			// Remove change set.
			delete(ctx.Changes, key)
		}
	}
}

func (d *SliceMaster) Apply(shard *Slice, l *raft.Log) interface{} {
	data := l.Data
	leader := shard.raft.State() == raft.Leader

	var complete bool
	var args [][]byte
	var reply []byte
	var err error

	for {
		complete, args, _, data, err = redcon.ReadNextCommand(data, args[:0])

		if err != nil {
			shard.Logger.Panic().Err(err).Msgf("invalid raft log entry index %d", l.Index)
			break
		}

		// Do we need more data?
		if !complete {
			// Exit loop.
			break
		}

		if len(args) > 0 {
			name := strings.ToUpper(string(args[0]))
			switch name {
			case "SET":

			case "DEL":

			}

			// First argument is the Cmd string.
			//out = c.incoming(out, strings.ToUpper(string(args[0])), args)
		}
	}

	if leader {
		return reply
	} else {
		return true
	}
}

type SetCmd struct {
	cmd.WriteCmd

	Key   string
	Value string
}

func (c *SetCmd) Invoke(ctx *cmd.Context) []byte {
	prev, replaced, err := ctx.Set.Set((item.StringKey)(c.Key), c.Value, 0)

	if err != nil {
		if err == sliced.ErrNotFound {
			return ctx.AppendNull()
		} else {
			return ctx.AppendError("ERR data not available: " + err.Error())
		}
	} else {
		if replaced {
			return ctx.AppendBulkString(prev)
		} else {
			return ctx.AppendNull()
		}
	}
}

func (d *SliceMaster) Parse(ctx *cmd.Context) cmd.Command {
	//name := strings.ToUpper(castString(args[0]))
	args := ctx.Args
	key := ctx.Key

	var _cmd cmd.Command
	switch ctx.Name {
	default:
		return cmd.RAW(redcon.AppendError(nil, "ERR invalid command"))

		//case txModeName:
		//	switch strings.ToUpper(key) {
		//	default:
		//		ctx.Conn.durability = cmd.Medium
		//	case "0":
		//		ctx.Conn.durability = cmd.Medium
		//	case "MEDIUM":
		//		ctx.Conn.durability = cmd.Medium
		//	case "1":
		//		ctx.Conn.durability = cmd.High
		//	case "2":
		//		ctx.Conn.durability = cmd.High
		//	case "HIGH":
		//		ctx.Conn.durability = cmd.High
		//	}

	case getName:
		if len(ctx.Args) != 2 {
			return cmd.ERR("ERR 1 parameter expected")
		} else {
			shard := d.sliceForKey(key)
			if shard == nil {
				return cmd.ERR(fmt.Sprintf("ERR shard not on node"))
			} else {
				var _cmd cmd.Command
				shard.ReadSortedSet(key, func(set *item.SortedSet) {
					val, err := set.Get(item.StringKey(key))
					if err != nil {
						if err == sliced.ErrNotFound {
							_cmd = cmd.RAW(redcon.AppendNull(nil))
						} else {
							_cmd = cmd.ERR("ERR data not available " + err.Error())
						}
					} else {
						_cmd = cmd.RAW(redcon.AppendBulkString(nil, val))
					}
				})
				return _cmd
			}
		}

	case setName:
		if len(args) < 3 {
			return cmd.ERR("ERR invalid parameters")
		}

		value := string(args[2])

		shard := d.sliceForKey(key)
		if shard == nil {
			return cmd.ERR(fmt.Sprintf("ERR shard not on node"))
		} else {
			ctx.SliceID = shard.ID()
			return &SetCmd{
				Key:      key,
				Value:    value,
			}
			//var _cmd cmd.Command
			//shard.WriteSortedSet(key, func(set *item.SortedSet) {
			//	val, replaced, err := set.Set(item.StringKey(key), value, 0)
			//	if err != nil {
			//		if err == sliced.ErrNotFound {
			//			_cmd = cmd.RAW(redcon.AppendNull(nil))
			//		} else {
			//			_cmd = cmd.ERR("ERR data not available " + err.Error())
			//		}
			//	} else {
			//		if replaced {
			//			_cmd = cmd.BulkString(val)
			//		} else {
			//			_cmd = cmd.RAW(redcon.AppendNull(nil))
			//		}
			//	}
			//})
			//return _cmd
		}

	case delName:
		return cmd.WRITE

	case "SHRINK":

		d.mu.RLock()
		slices := make([]*Slice, len(d.slices))
		copy(slices, d.slices)
		d.mu.RUnlock()

		for _, shard := range slices {
			shard.Shrink()
		}

		return cmd.OK()

		//case "KEYS":
		//	return cmd.RAW(redcon.AppendInt(nil, int64(d.slices[0].db.NumKeys())))

	case raftInstallSnapshotName:
		if len(args) < 3 {
			return cmd.ERR("ERR 2 parameters expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return cmd.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.SliceForSlot(id)
				if shard == nil {
					return cmd.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					return shard.HandleInstallSnapshot(ctx, args[2])
				}
			}
		}

	case raftChunkName:

	case raftDoneName:

	case raftJoinName:
		if len(args) < 3 {
			return cmd.ERR("ERR 2 parameters")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return cmd.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.SliceForSlot(id)
				if shard == nil {
					return cmd.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					addr := string(args[2])
					addr = strings.Trim(addr, "")
					if err := shard.Join(addr, addr); err != nil {
						return cmd.ERR(fmt.Sprintf("ERR %s", err.Error()))
					} else {
						return cmd.RAW(redcon.AppendOK(nil))
					}
				}
			}
		}

	case raftRemoveName:
		if len(args) < 3 {
			return cmd.ERR("ERR 2 parameters")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return cmd.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.SliceForSlot(id)
				if shard == nil {
					return cmd.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					addr := string(args[2])
					addr = strings.Trim(addr, "")
					if err := shard.Leave(addr, addr); err != nil {
						return cmd.ERR(fmt.Sprintf("ERR %s", err.Error()))
					} else {
						return cmd.RAW(redcon.AppendOK(nil))
					}
				}
			}
		}

	case raftAppendName:
		if len(args) < 3 {
			return cmd.ERR("ERR 2 parameters expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return cmd.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.SliceForSlot(id)
				if shard == nil {
					return cmd.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					b, err := shard.AppendEntries(nil, args)
					if err != nil {
						return cmd.ERR(fmt.Sprintf("ERR %s", err.Error()))
					} else {
						return cmd.RAW(b)
					}
				}
			}
		}

	case raftVoteName:
		if len(args) < 3 {
			return cmd.ERR("ERR 2 parameters expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return cmd.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.SliceForSlot(id)
				if shard == nil {
					return cmd.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					b, err := shard.RequestVote(nil, args)
					if err != nil {
						return cmd.ERR(fmt.Sprintf("ERR %s", err.Error()))
					} else {
						return cmd.RAW(b)
					}
				}
			}
		}

	case raftLeaderName:
		return cmd.RAW(d.raftSlice(nil, args, func(shard *Slice) []byte {
			leader, err := shard.Leader()
			if err != nil {
				return redcon.AppendError(nil, "ERR "+err.Error())
			} else {
				return redcon.AppendBulkString(nil, leader)
			}
		}))

	case raftStatsName:
		return cmd.RAW(d.raftSlice(nil, args, func(shard *Slice) []byte {
			stats := shard.Stats()
			keys := make([]string, 0, len(stats))
			for key := range stats {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			var out []byte
			out = redcon.AppendArray(out, len(keys)*2)
			for _, key := range keys {
				out = redcon.AppendBulkString(out, key)
				out = redcon.AppendBulkString(out, stats[key])
			}
			return out
		}))

	case raftStateName:
		return cmd.RAW(d.raftSlice(nil, args, func(shard *Slice) []byte {
			state := shard.State()
			return redcon.AppendBulkString(nil, state.String())
		}))

	case raftShrinkName:
		return cmd.RAW(d.raftSlice(nil, args, func(slice *Slice) []byte {
			err := slice.ShrinkLog()
			if err != nil {
				return redcon.AppendError(nil, "ERR "+err.Error())
			} else {
				return redcon.AppendOK(nil)
			}
		}))

	case raftSnapshotName:
		return cmd.RAW(d.raftSlice(nil, args, func(shard *Slice) []byte {
			f := shard.raft.Snapshot()
			_ = f
			return redcon.AppendOK(nil)
		}))

	case raftSnapshotsName:
		return cmd.RAW(d.raftSlice(nil, args, func(shard *Slice) []byte {
			f, err := shard.snapshots.List()
			if err != nil {
				return redcon.AppendError(nil, err.Error())
			} else {
				var out []byte
				out = redcon.AppendArray(out, len(f))
				for _, item := range f {
					out = redcon.AppendArray(out, 4)
					out = redcon.AppendBulkString(out, "ID")
					out = redcon.AppendBulkString(out, item.ID)
					out = redcon.AppendBulkString(out, "Index")
					out = redcon.AppendInt(out, int64(item.Index))
				}
				return out
			}
		}))
	}
	return _cmd
}

func (d *SliceMaster) raftSlice(out []byte, args [][]byte, fn func(shard *Slice) []byte) []byte {
	id := -1
	var err error
	if len(args) == 1 {
		id = -1
	} else {
		id, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return redcon.AppendError(out, "ERR invalid int param for shard id")
		}
	}
	shard := d.SliceForSlot(id)
	if shard == nil {
		if id == -1 {
			return redcon.AppendError(out, "ERR master not on node")
		} else {
			return redcon.AppendError(out, fmt.Sprintf("ERR shard %d not on node", id))
		}
	} else {
		return fn(shard)
	}
}

type setCmd struct {
	cmd.WriteCmd
}

//func (s *setCmd) Invoke(b []byte) []byte {
//	return b
//}
