package db

import (
	"github.com/pointc-io/ipdb/service"
	"github.com/pointc-io/ipdb"

	"fmt"
	"path/filepath"
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

	id      string
	path    string
	cluster *Shard
	shards  []*Shard
}

func NewDB(id, path string) *DB {
	d := &DB{
		id:     id,
		path:   path,
		shards: make([]*Shard, 0, 8),
	}

	d.BaseService = *service.NewBaseService(ipdb.Logger, "db", d)
	return d
}

func (d *DB) Get(key string) *Shard {
	return d.shards[Slot(key)%len(d.shards)]
}

func (d *DB) Shard(id int) *Shard {
	return d.shards[id]
}

func (d *DB) OnStart() error {
	var clusterPath string
	if d.path == ":memory:" {
		clusterPath = ":memory:"
	} else {
		clusterPath = filepath.Join(d.path, "cluster")
	}

	// Load cluster shard.
	d.cluster = NewShard(d.path == ":memory:", clusterPath, d.id)
	err := d.cluster.Start()
	if err != nil {
		d.Logger.Error().Err(err).Msg("cluster db start failed")
		return err
	}



	for i := 0; i < len(d.shards); i++ {
		ppath := d.path
		if ppath != ":memory:" {
			ppath = fmt.Sprintf("%s%sp-%d.db", d.path, string(filepath.Separator), i)
		}
		d.shards[i] = NewShard(false, ppath, "")
	}

	for i := 0; i < len(d.shards); i++ {
		err := d.shards[i].Start()

		if err != nil {
			for a := 0; a < i; a++ {
				d.shards[a].Stop()
			}

			return err
		}
	}
	return nil
}

func (d *DB) OnStop() {
	for i := 0; i < len(d.shards); i++ {
		err := d.shards[i].Stop()

		if err != nil {
			d.Logger.Error().Err(err)
		} else {
			d.shards[i].Wait()
		}
	}
}
