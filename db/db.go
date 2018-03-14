package db

import (
	"path/filepath"
	"os"
	"sync"
	"fmt"
	"strings"
	"sort"
	"strconv"

	"github.com/pointc-io/ipdb"
	"github.com/pointc-io/ipdb/redcon"
	"github.com/pointc-io/ipdb/redcon/ev"
	"github.com/pointc-io/ipdb/service"
	"github.com/hashicorp/raft"
	"github.com/pointc-io/ipdb/db/buntdb"
	"github.com/tidwall/btree"
	"unsafe"
)

const (
	getName    = "GET"
	setName    = "SET"
	delName    = "DEL"
	txModeName = "TX"

	raftInstallName   = "RAFTINSTALL"
	raftAppendName    = "RAFTAPPEND"
	raftVoteName      = "RAFTVOTE"
	raftChunkName     = "CHUNK"
	raftDoneName      = "DONE"
	raftSnapshotName  = "RAFTSNAPSHOT"
	raftSnapshotsName = "RAFTSNAPSHOTS"
	raftJoinName      = "RAFTJOIN"
	raftRemoveName    = "RAFTREMOVE"
	raftStatsName     = "RAFTSTATS"
	raftStateName     = "RAFTSTATE"
	raftLeaderName    = "RAFTLEADER"
	raftShrinkName    = "RAFTSHRINK"
)

var (
	raftInstall = []byte(raftInstallName)
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

// DB is broken up into Partitions that are assigned to a slot.
// There are 16384 slots.
// Partitions are balanced first across cores then nodes.

type Slots struct {
	Index  [16384]*Shard
	Shards []*Shard
}

// DB maintains cluster wide consensus which maintains shard raft groups.
// Each shard needs a minimum of 3 nodes.
// Shard selection is determined using a CRC16 on the key and follows the same paradigm as Redis Cluster.
type DB struct {
	service.BaseService

	host    string
	path    string
	bind    string
	cluster *Shard
	shards  []*Shard

	mu sync.RWMutex

	_map map[string]string

	tree *btree.BTree
}

func NewDB(host, path string) *DB {
	d := &DB{
		host:   host,
		path:   path,
		shards: make([]*Shard, 0, 8),
		_map:   make(map[string]string),
		tree:   btree.New(64, nil),
	}

	d.BaseService = *service.NewBaseService(sliced.Logger, "db", d)
	return d
}

func (d *DB) Get(key string) *Shard {
	slot := redcon.Slot(key)
	d.mu.RLock()
	idx := slot % len(d.shards)
	shard := d.shards[idx]
	d.mu.RUnlock()
	return shard
}

func (d *DB) ShardLen() int {
	d.mu.RLock()
	l := len(d.shards)
	d.mu.RUnlock()
	return l
}

func (d *DB) ShardID(key string) int {
	slot := redcon.Slot(key)
	d.mu.RLock()
	idx := slot % len(d.shards)
	d.mu.RUnlock()
	return idx
}

func (d *DB) Shard(id int) *Shard {
	var shard *Shard
	d.mu.RLock()
	if id == -1 {
		shard = d.cluster
		d.mu.RUnlock()
		return shard
	}
	if id < 0 || id >= len(d.shards) {
		d.mu.RUnlock()
		return nil
	}
	shard = d.shards[id]
	d.mu.RUnlock()
	return shard
}

func (d *DB) shardPath(id int) (string, error) {
	if d.path == ":memory:" {
		return d.path, nil
	} else {
		var path string
		if id == -1 {
			path = filepath.Join(d.path, "cluster")
		} else {
			path = filepath.Join(d.path, "shards", fmt.Sprintf("%d", id))
		}
		return path, os.MkdirAll(path, 0700)
	}
}

func (d *DB) OnStart() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	clusterPath, err := d.shardPath(-1)

	if err != nil {
		d.Logger.Fatal().Err(err).Msgf("failed to mkdirs for \"%s\"", clusterPath)
		return err
	}

	// Load cluster shard.
	d.cluster = NewShard(-1, d.path == ":memory:", clusterPath, d.host, d)
	err = d.cluster.Start()
	if err != nil {
		d.Logger.Error().Err(err).Msg("cluster db start failed")
		return err
	}

	if d.cluster.db.IsEmpty() {
		for i := 0; i < 1; i++ {
			shardPath, err := d.shardPath(i)

			if err != nil {
				d.Logger.Fatal().Err(err).Msgf("failed to mkdirs for \"%s\"", clusterPath)
				return err
			}

			// Create the first shard.
			shard := NewShard(i, d.path == ":memory:", shardPath, d.host, d)
			d.shards = append(d.shards, shard)
		}
	}

	for i := 0; i < len(d.shards); i++ {
		err := d.shards[i].Start()

		if err != nil {
			for a := 0; a < i; a++ {
				d.shards[a].Stop()
			}
			d.Logger.Error().Err(err).Msgf("failed to start shard %d", i)
			return err
		}
	}
	return nil
}

func (d *DB) OnStop() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i := 0; i < len(d.shards); i++ {
		err := d.shards[i].Stop()

		if err != nil {
			d.Logger.Error().Err(err)
		} else {
			d.shards[i].Wait()
		}
	}
}

type handler DB

func (d *DB) Commit(ctx *evred.CommandContext) {
	if len(ctx.Changes) == 0 {
		return
	}

LOOP:
	for key, set := range ctx.Changes {
		shard := d.Shard(key)
		if shard == nil {
			for i := 0; i < len(set.Cmds); i++ {
				ctx.Out = redcon.AppendError(ctx.Out, fmt.Sprintf("ERR shard %d not on node", key))
			}
		} else {
			if ctx.Conn.Durability == evred.High {
				future := shard.raft.Apply(set.Data, raftTimeout)

				var err error
				if err = future.Error(); err != nil {
					for i := 0; i < len(set.Cmds); i++ {
						ctx.Out = ctx.AppendError("ERR " + err.Error())
					}
				} else if resp := future.Response(); resp != nil {
					if buf, ok := resp.([]byte); ok {
						ctx.Out = append(ctx.Out, buf...)
					} else {
						switch t := resp.(type) {
						case string:
							ctx.Out = ctx.AppendBulkString(t)
						default:
							for i := 0; i < len(set.Cmds); i++ {
								ctx.Out = ctx.AppendError(fmt.Sprintf("ERR apply return type %v", t))
							}
						}
					}
				} else {
					for i := 0; i < len(set.Cmds); i++ {
						ctx.Out = ctx.AppendOK()
					}
				}
			} else {
				tx, err := shard.db.Begin(true)

				if err != nil {
					for i := 0; i < len(set.Cmds); i++ {
						ctx.Out = ctx.AppendError(fmt.Sprintf("ERR failed to start tx %v", err.Error()))
					}
					delete(ctx.Changes, key)
					continue LOOP
				}

				begin := len(ctx.Out)
				ctx.DB = shard.db
				ctx.Tx = tx
				for i := 0; i < len(set.Cmds); i++ {
					ctx.Out = set.Cmds[i].Invoke(ctx)
				}

				err = tx.Commit()
				if err != nil {
					ctx.Out = ctx.Out[:begin]
					for i := 0; i < len(set.Cmds); i++ {
						ctx.Out = redcon.AppendError(ctx.Out, "ERR "+err.Error())
					}
				} else {
					// Apply inside lock.
					shard.raft.Apply(set.Data, raftTimeout)
				}

				// Cleanup
				ctx.DB = nil
				ctx.Tx = nil

				// Remove change set.
				delete(ctx.Changes, key)
			}
		}
	}
}

//func (d *DB) Commit(conn *evred.Conn, shard int, commitlog []byte) (raft.ApplyFuture, error) {
//	s := d.Shard(shard)
//	if s == nil {
//		return nil, ErrShardNotExists
//	}
//	return s.raft.Apply(commitlog, raftTimeout), nil
//}

func (d *DB) Apply(shard *Shard, l *raft.Log) interface{} {
	tx, err := shard.db.Begin(true)
	if err != nil {
		shard.Logger.Panic().Err(err)
	}
	data := l.Data
	leader := shard.raft.State() == raft.Leader

	var complete bool
	var args [][]byte
	var reply []byte

	for {
		complete, args, _, data, err = redcon.ReadNextCommand(data, args[:0])

		if err != nil {
			tx.Rollback()
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
			case "SET2":
				key := string(args[1])
				d.mu.Lock()
				item := d.tree.Get(&dbItem{key: key})
				ok := false
				var prev string
				var it *dbItem
				if item != nil {
					it = item.(*dbItem)
					prev = it.value
					ok = true
				} else {
					it = &dbItem{key: key, value: string(args[2])}
					d.tree.ReplaceOrInsert(it)
				}
				_ = prev
				//prev, ok := d._map[key]
				//d._map[key] = string(args[2])
				d.mu.Unlock()
				if !ok {
					reply = redcon.AppendNull(reply)
				} else {
					reply = redcon.AppendBulkString(reply, "hi")
				}

				//key := string(args[1])
				//prev, ok := d._map[key]
				//d._map[key] = string(args[2])
				//if leader {
				//	if !ok {
				//		reply = redcon.AppendNull(reply)
				//	} else {
				//		reply = redcon.AppendBulkString(reply, prev)
				//	}
				//}

			case "SET":
				prev, replaced, err := tx.Set(string(args[1]), string(args[2]), nil)

				if leader {
					if err != nil {
						reply = redcon.AppendError(reply, fmt.Sprintf("ERR %s", err.Error()))
					} else if replaced {
						reply = redcon.AppendBulkString(reply, prev)
					} else {
						reply = redcon.AppendNull(reply)
					}
				}

			case "DEL":
				prev, err := tx.Delete(string(args[1]))

				if leader {
					if err == buntdb.ErrNotFound {
						reply = redcon.AppendNull(reply)
					} else {
						reply = redcon.AppendBulkString(reply, prev)
					}
				}
			}

			// First argument is the Cmd string.
			//out = c.incoming(out, strings.ToUpper(string(args[0])), args)
		}
	}

	tx.Commit()
	if leader {
		return reply
	} else {
		return true
	}
}

type dbItem struct {
	key   string
	value string
}

func (item *dbItem) Less(than btree.Item, ctx interface{}) bool {
	return true
}

func castString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

type SetCmd struct {
	evred.WriteCmd

	Key   string
	Value string
}

func (c *SetCmd) Invoke(ctx *evred.CommandContext) []byte {
	val, replaced, err := ctx.Tx.Set(c.Key, c.Value, nil)
	if err != nil {
		if err == buntdb.ErrNotFound {
			return ctx.AppendNull()
		} else {
			return ctx.AppendError("ERR data not available " + err.Error())
		}
	} else {
		if replaced {
			return ctx.AppendBulkString(val)
		} else {
			return ctx.AppendNull()
		}
	}
}

func (d *DB) Parse(ctx *evred.CommandContext) evred.Command {
	//name := strings.ToUpper(castString(args[0]))
	args := ctx.Args
	key := ctx.Key

	var cmd evred.Command
	switch ctx.Name {
	default:
		return evred.RAW(redcon.AppendError(nil, "ERR invalid command"))

	case txModeName:
		switch strings.ToUpper(key) {
		default:
			ctx.Conn.Durability = evred.Medium
		case "0":
			ctx.Conn.Durability = evred.Medium
		case "MEDIUM":
			ctx.Conn.Durability = evred.Medium
		case "1":
			ctx.Conn.Durability = evred.High
		case "2":
			ctx.Conn.Durability = evred.High
		case "HIGH":
			ctx.Conn.Durability = evred.High
		}

	case getName:
		if len(ctx.Args) != 2 {
			return evred.ERR("ERR 1 parameter expected")
		} else {
			shard := d.Get(key)
			if shard == nil {
				return evred.ERR(fmt.Sprintf("ERR shard not on node"))
			} else {
				val, err := shard.db.Get(key)
				if err != nil {
					if err == buntdb.ErrNotFound {
						return evred.RAW(redcon.AppendNull(nil))
					} else {
						return evred.ERR("ERR data not available " + err.Error())
					}
				} else {
					return evred.RAW(redcon.AppendBulkString(nil, val))
				}
			}
		}

	case setName:
		if len(args) < 3 {
			return evred.ERR("ERR invalid parameters")
		}

		//key := string(args[1])
		//d.mu.Lock()
		//item := d.tree.Get(&dbItem{key:key})
		//ok := false
		//var prev string
		//var it *dbItem
		//if item != nil {
		//	it = item.(*dbItem)
		//	prev = it.value
		//	ok = true
		//} else {
		//	it = &dbItem{key: key, value: string(args[2])}
		//	d.tree.ReplaceOrInsert(it)
		//}
		////prev, ok := d._map[key]
		////d._map[key] = string(args[2])
		//d.mu.Unlock()
		//if !ok {
		//	return evred.RAW(redcon.AppendNull(nil))
		//} else {
		//	return evred.RAW(redcon.AppendBulkString(nil, prev))
		//}

		value := string(args[2])

		shard := d.Get(key)
		if shard == nil {
			return evred.ERR(fmt.Sprintf("ERR shard not on node"))
		} else {
			ctx.ShardID = shard.id
			return &SetCmd{Key: key, Value: value}

			//val, err := shard.db.Set(string(key), value)
			//if err != nil {
			//	if err == buntdb.ErrNotFound {
			//		return evred.AsyncWrite(redcon.AppendNull(nil))
			//	} else {
			//		return evred.ERR("ERR data not available " + err.Error())
			//	}
			//} else {
			//	return evred.AsyncWrite(redcon.AppendBulkString(nil, val))
			//}
		}

		//prev, replaced, err := tx.Set(key, value, nil)

		//if leader {
		//if err != nil {
		//	out = redcon.AppendError(out, fmt.Sprintf("ERR %s", err.Error()))
		//} else if replaced {
		//	out = redcon.AppendBulkString(out, prev)
		//} else {
		//	out = redcon.AppendNull(out)
		//}

		//return evred.RAW(redcon.AppendOK(nil))
		//return evred.WRITE

	case delName:
		return evred.WRITE

	case "SHRINK":

		d.mu.RLock()
		shards := make([]*Shard, len(d.shards))
		copy(shards, d.shards)
		d.mu.RUnlock()

		for _, shard := range shards {
			shard.Shrink()
		}

		return evred.OK()

	case "KEYS":
		return evred.RAW(redcon.AppendInt(nil, int64(d.shards[0].db.NumKeys())))

	case raftInstallName:
		if len(args) < 3 {
			return evred.ERR("ERR 2 parameters expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return evred.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.Shard(id)
				if shard == nil {
					return evred.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					return shard.HandleInstallSnapshot(ctx.Conn, args[2])
				}
			}
		}

	case raftChunkName:

	case raftDoneName:

	case raftJoinName:
		if len(args) < 3 {
			return evred.ERR("ERR 2 parameters")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return evred.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.Shard(id)
				if shard == nil {
					return evred.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					addr := string(args[2])
					addr = strings.Trim(addr, "")
					if err := shard.Join(addr, addr); err != nil {
						return evred.ERR(fmt.Sprintf("ERR %s", err.Error()))
					} else {
						return evred.RAW(redcon.AppendOK(nil))
					}
				}
			}
		}

	case raftRemoveName:
		if len(args) < 3 {
			return evred.ERR("ERR 2 parameters")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return evred.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.Shard(id)
				if shard == nil {
					return evred.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					addr := string(args[2])
					addr = strings.Trim(addr, "")
					if err := shard.Leave(addr, addr); err != nil {
						return evred.ERR(fmt.Sprintf("ERR %s", err.Error()))
					} else {
						return evred.RAW(redcon.AppendOK(nil))
					}
				}
			}
		}

	case raftAppendName:
		if len(args) < 3 {
			return evred.ERR("ERR 2 parameters expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return evred.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.Shard(id)
				if shard == nil {
					return evred.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					b, err := shard.AppendEntries(nil, args)
					if err != nil {
						return evred.ERR(fmt.Sprintf("ERR %s", err.Error()))
					} else {
						return evred.RAW(b)
					}
				}
			}
		}

	case raftVoteName:
		if len(args) < 3 {
			return evred.ERR("ERR 2 parameters expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				return evred.ERR("ERR invalid int param for shard id")
			} else {
				shard := d.Shard(id)
				if shard == nil {
					return evred.ERR(fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					b, err := shard.RequestVote(nil, args)
					if err != nil {
						return evred.ERR(fmt.Sprintf("ERR %s", err.Error()))
					} else {
						return evred.RAW(b)
					}
				}
			}
		}

	case raftLeaderName:
		return evred.RAW(d.raftShard(nil, args, func(shard *Shard) []byte {
			leader, err := shard.Leader()
			if err != nil {
				return redcon.AppendError(nil, "ERR "+err.Error())
			} else {
				return redcon.AppendBulkString(nil, leader)
			}
		}))

	case raftStatsName:
		return evred.RAW(d.raftShard(nil, args, func(shard *Shard) []byte {
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
		return evred.RAW(d.raftShard(nil, args, func(shard *Shard) []byte {
			state := shard.State()
			return redcon.AppendBulkString(nil, state.String())
		}))

	case raftShrinkName:
		return evred.RAW(d.raftShard(nil, args, func(shard *Shard) []byte {
			err := shard.ShrinkLog()
			if err != nil {
				return redcon.AppendError(nil, "ERR "+err.Error())
			} else {
				return redcon.AppendOK(nil)
			}
		}))

	case raftSnapshotName:
		return evred.RAW(d.raftShard(nil, args, func(shard *Shard) []byte {
			f := shard.raft.Snapshot()
			_ = f
			return redcon.AppendOK(nil)
		}))

	case raftSnapshotsName:
		return evred.RAW(d.raftShard(nil, args, func(shard *Shard) []byte {
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
	return cmd
}

func (d *DB) raftShard(out []byte, args [][]byte, fn func(shard *Shard) []byte) []byte {
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
	shard := d.Shard(id)
	if shard == nil {
		if id == -1 {
			return redcon.AppendError(out, "ERR cluster not on node")
		} else {
			return redcon.AppendError(out, fmt.Sprintf("ERR shard %d not on node", id))
		}
	} else {
		return fn(shard)
	}
}

type setCmd struct {
	evred.WriteCmd
}

//func (s *setCmd) Invoke(b []byte) []byte {
//	return b
//}
