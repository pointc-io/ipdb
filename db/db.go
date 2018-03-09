package db

import (
	"path/filepath"
	"os"

	"github.com/pointc-io/ipdb"
	"github.com/pointc-io/ipdb/service"
	"sync"
	"fmt"
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

	mu sync.Mutex
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
	if id == -1 {
		return d.cluster
	}
	if id < 0 || id >= len(d.shards) {
		return nil
	}
	return d.shards[id]
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
	d.cluster = NewShard(-1, d.path == ":memory:", clusterPath, d.id)
	err = d.cluster.Start()
	if err != nil {
		d.Logger.Error().Err(err).Msg("cluster db start failed")
		return err
	}

	if d.cluster.db.IsEmpty() {
		shardPath, err := d.shardPath(-1)

		if err != nil {
			d.Logger.Fatal().Err(err).Msgf("failed to mkdirs for \"%s\"", clusterPath)
			return err
		}

		// Create the first shard.
		shard := NewShard(0, d.path == ":memory:", shardPath, d.id)
		d.shards = append(d.shards, shard)
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
	for i := 0; i < len(d.shards); i++ {
		err := d.shards[i].Stop()

		if err != nil {
			d.Logger.Error().Err(err)
		} else {
			d.shards[i].Wait()
		}
	}
}
