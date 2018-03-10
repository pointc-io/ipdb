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
)

// DB is broken up into Partitions that are assigned to a slot.
// There are 16384 slots.
// Partitions are balanced first across cores then nodes.

type Slots struct {
	Index  [16384]*Shard
	Shards []*Shard
}

type IShard interface {
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
}

func NewDB(host, path string) *DB {
	d := &DB{
		host:   host,
		path:   path,
		shards: make([]*Shard, 0, 8),
	}

	d.BaseService = *service.NewBaseService(butterd.Logger, "db", d)
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
	return reply
}

func (d *DB) Parse(conn *evred.Conn, packet []byte, args [][]byte) evred.Command {
	name := strings.ToUpper(string(args[0]))

	var cmd evred.Command
	switch name {
	default:
		return evred.RAW(redcon.AppendError(nil, "ERR invalid command"))
	case "GET":
		if len(args) != 2 {
			return evred.ERR("ERR 1 parameter expected")
		} else {
			key := string(args[1])
			_ = key
			//out = redcon.AppendNull(out)
			shard := d.Get(key)
			if shard == nil {
				return evred.ERR(fmt.Sprintf("ERR shard not on node"))
			} else {
				tx, err := shard.db.Begin(false)
				if err != nil {
					return evred.ERR("ERR data not available " + err.Error())
				} else {
					val, err := tx.Get(key)
					tx.Rollback()
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
		}

		return evred.RAW(redcon.AppendOK(nil))

	case "SET":
		return &evred.WriteCmd{}

	case "DEL":
		return &evred.WriteCmd{}

	case "RAFTJOIN":
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

	case "RAFTLEAVE":
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

	case "RAFTAPPEND":
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

	case "RAFTVOTE":
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

	case "RAFTINSTALL":
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
					conn.Detach(shard.HandleInstallSnapshot(args[2]))
				}
			}
		}

	case "RAFTLEADER":
		return evred.RAW(d.raftShard(nil, args, func(shard *Shard) []byte {
			leader, err := shard.Leader()
			if err != nil {
				return redcon.AppendError(nil, "ERR "+err.Error())
			} else {
				return redcon.AppendBulkString(nil, leader)
			}
		}))

	case "RAFTSTATS":
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

	case "RAFTSTATE":
		return evred.RAW(d.raftShard(nil, args, func(shard *Shard) []byte {
			state := shard.State()
			return redcon.AppendBulkString(nil, state.String())
		}))

	case "RAFTSHRINK":
		return evred.RAW(d.raftShard(nil, args, func(shard *Shard) []byte {
			err := shard.ShrinkLog()
			if err != nil {
				return redcon.AppendError(nil, "ERR "+err.Error())
			} else {
				return redcon.AppendOK(nil)
			}
		}))
	}
	return cmd
}

func (d *DB) Commit(conn *evred.Conn, shard int, commitlog []byte) (raft.ApplyFuture, error) {
	s := d.Shard(shard)
	if s == nil {
		return nil, ErrShardNotExists
	}
	return s.raft.Apply(commitlog, raftTimeout), nil
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
