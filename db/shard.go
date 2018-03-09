// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package db

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
	"errors"

	"github.com/hashicorp/raft"
	"github.com/pointc-io/ipdb/db/store"
	"github.com/pointc-io/ipdb/db/buntdb"
	"github.com/pointc-io/ipdb/service"
	"github.com/pointc-io/ipdb"
	"log"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

var (
	ErrLogNotShrinkable = errors.New("ERR log not shrinkable")
	ErrShardNotExists   = errors.New("ERR shard not exists")
	ErrNotLeader        = errors.New("ERR not leader")
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Shard is a partition of the total keyspace, where all changes are made via Raft consensus.
// The entire database is a single BTree that may contain many sparse BTree indexes.
// A shard is completely independent from every other shard.
// Each shard has it's own Raft group and replication.
type Shard struct {
	service.BaseService

	id           int
	Path         string
	Bind         string
	enableSingle bool
	localID      string

	mu sync.Mutex
	m  map[string]string // The key-value store for the system.

	raft      *raft.Raft // The consensus mechanism
	snapshots raft.SnapshotStore
	transport *RaftTransport
	trans     raft.Transport
	store     bigStore
	db        *buntdb.DB
}

// bigStore represents a raft store that conforms to
// raft.PeerStore, raft.LogStore, and raft.StableStore.
type bigStore interface {
	Close() error
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	GetLog(idx uint64, log *raft.Log) error
	StoreLog(log *raft.Log) error
	StoreLogs(logs []*raft.Log) error
	DeleteRange(min, max uint64) error
	Set(k, v []byte) error
	Get(k []byte) ([]byte, error)
	SetUint64(key []byte, val uint64) error
	GetUint64(key []byte) (uint64, error)
	Peers() ([]string, error)
	SetPeers(peers []string) error
}

type shrinkable interface {
	Shrink() error
}

// New returns a new Shard.
func NewShard(enableSingle bool, path, localID string) *Shard {
	s := &Shard{
		Path:         path,
		localID:      localID,
		enableSingle: enableSingle,
		m:            make(map[string]string),
	}
	var name string
	if s.id < 0 {
		name = "clusterdb"
	} else {
		name = fmt.Sprintf("shard-%d", s.id)
	}

	s.BaseService = *service.NewBaseService(ipdb.Logger, name, s)
	return s
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *Shard) OnStart() error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.localID)
	config.Logger = log.New(s.Logger, "", 0)
	var err error

	// Open database
	var dbpath string
	if s.Path == ":memory:" {
		dbpath = ":memory:"
	} else {
		dbpath = filepath.Join(s.Path, "data.db")
	}
	s.db, err = buntdb.Open(dbpath)
	if err != nil {
		s.db.Close()
		s.Logger.Error().Err(err).Msg("db open failed")
		return err
	}

	// Create Transport
	if s.Path == ":memory:" {
		_, t := raft.NewInmemTransport("")
		s.trans = t
	} else {
		s.transport = NewRaftTransport(s)
		s.trans = s.transport

		err := s.transport.Start()
		if err != nil {
			s.Logger.Error().Err(err).Msg("raft transport start failed")
			return err
		}
	}

	// Create Snapshot Store
	if s.Path == ":memory:" {
		s.snapshots = raft.NewInmemSnapshotStore()
	} else {
		// Create the snapshot store. This allows the Raft to truncate the log.
		s.snapshots, err = raft.NewFileSnapshotStore(s.Path, retainSnapshotCount, os.Stdout)
		if err != nil {
			s.transport.Stop()
			s.Logger.Error().Err(err).Msg("file snapshot store failed")
			return fmt.Errorf("file snapshot store: %s", err)
		}
	}

	// Create Log Store
	var logpath string
	if s.Path == ":memory:" {
		logpath = ":memory:"
	} else {
		logpath = filepath.Join(s.Path, "raft.db")
	}
	// Create the log store and stable store.
	s.store, err = raftfastlog.NewFastLogStore(
		logpath,
		raftfastlog.Medium,
		ipdb.Logger.With().Str("logger", fmt.Sprintf("shard-%d-log", s.id)).Logger(),
	)
	if err != nil {
		s.transport.Stop()
		s.Logger.Error().Err(err).Msg("log store failed")
		return fmt.Errorf("new log store: %s", err)
	}

	// Instantiate the Raft systems.
	s.raft, err = raft.NewRaft(config, (*fsm)(s), s.store, s.store, s.snapshots, s.transport)
	if err != nil {
		s.db.Close()
		s.transport.Stop()
		s.store.Close()
		s.Logger.Error().Err(err).Msg("new raft failed")
		return fmt.Errorf("new raft: %s", err)
	}

	if s.enableSingle {
		//config.StartAsLeader = true
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: s.transport.LocalAddr(),
				},
			},
		}
		s.raft.BootstrapCluster(configuration)
	}

	return nil
}

func (s *Shard) OnStop() {
	if err := s.raft.Shutdown().Error(); err != nil {
		s.Logger.Error().Err(err).Msg("raft shutdown error")
	}

	if s.transport != nil {
		if err := s.transport.Close(); err != nil {
			s.Logger.Error().Err(err).Msg("raft transport close error")
		}
	}
	if trans, ok := s.trans.(*raft.InmemTransport); ok {
		if err := trans.Close(); err != nil {
			s.Logger.Error().Err(err).Msg("raft in-memory transport close error")
		}
	}
	if err := s.store.Close(); err != nil {
		s.Logger.Error().Err(err).Msg("logstore close error")
	}
	if err := s.db.Close(); err != nil {
		s.Logger.Error().Err(err).Msg("db close error")
	}
}

func (s *Shard) Snapshot() error {
	f := s.raft.Snapshot()
	if err := f.Error(); err != nil {
		return err
	}
	return nil
}

// "RAFTSHRINK" client command.
func (s *Shard) ShrinkLog() error {
	if s, ok := s.store.(shrinkable); ok {
		err := s.Shrink()
		if err != nil {
			return err
		}
		return nil
	}
	return ErrLogNotShrinkable
}

// Only for RaftTransport RAFTAPPEND
func (s *Shard) AppendEntries(o []byte, args [][]byte) ([]byte, error) {
	return s.transport.handleAppendEntries(o, args)
}

// Only for RaftTransport RAFTVOTE
func (s *Shard) RequestVote(o []byte, args [][]byte) ([]byte, error) {
	return s.transport.handleRequestVote(o, args)
}

// Only for RaftTransport RAFTINSTALL
func (s *Shard) HandleInstallSnapshot(arg []byte) func(c net.Conn) {
	return s.transport.HandleInstallSnapshotFn(arg)
}

// "SHRINK" client command.
func (c *Shard) Shrink() error {
	if c.db != nil {
		err := c.db.Shrink()
		if err != nil {
			return err
		}
		return nil
	}
	return ErrLogNotShrinkable
}

func (s *Shard) Leader() (string, error) {
	return string(s.raft.Leader()), nil
}

func (s *Shard) Stats() map[string]string {
	return s.raft.Stats()
}

func (s *Shard) State() raft.RaftState {
	return s.raft.State()
}

// Get returns the value for the given key.
func (s *Shard) Get(key string) (string, error) {
	var val string = ""
	var err error
	s.db.View(func(tx *buntdb.Tx) error {
		val, err = tx.Get(key)
		return err
	})
	return val, err
}

// Set sets the value for the given key.
func (s *Shard) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Shard) Update(cmd string) (interface{}, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	err := s.db.Update(func(tx *buntdb.Tx) error {
		return nil
	})
	return nil, err
}

// Delete deletes the given key.
func (s *Shard) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Shard) Join(nodeID, addr string) error {
	s.Logger.Info().Str("node", nodeID).Str("addr", addr).Msg("node join request")

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		s.Logger.Info().Str("node", nodeID).Str("addr", addr).Err(f.Error()).Msg("node join failed")
		return f.Error()
	}
	s.Logger.Info().Str("node", nodeID).Str("addr", addr).Msg("node joined successfully")
	return nil
}

func (s *Shard) Leave(nodeID, addr string) error {
	s.Logger.Info().Str("node", nodeID).Str("addr", addr).Msg("node leave request")

	f := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if f.Error() != nil {
		s.Logger.Info().Str("node", nodeID).Str("addr", addr).Err(f.Error()).Msg("node leave failed")
		return f.Error()
	}
	s.Logger.Info().Str("node", nodeID).Str("addr", addr).Msg("node left successfully")
	return nil
}

type fsm Shard

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		f.Logger.Panic().Msgf("failed to unmarshal command: %s", err.Error())
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

type fsmSnapshot struct {
	store map[string]string
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
