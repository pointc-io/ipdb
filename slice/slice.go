// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the master agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package slice

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pointc-io/sliced"
	cmd "github.com/pointc-io/sliced/command"
	"github.com/pointc-io/sliced/item"
	"github.com/pointc-io/sliced/service"
	"github.com/pointc-io/sliced/slice/store"
	"github.com/rs/zerolog"
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

type Applier interface {
	Apply(shard *Slice, l *raft.Log) interface{}
}

// Slice is a partition of the total keyspace, where all changes are made via Raft consensus.
// The entire database is a single BTree that may contain many sparse BTree indexes.
// A shard is completely independent from every other shard.
// Each shard has it's own Raft group and replication.
type Slice struct {
	service.BaseService

	id           int
	low          int
	high         int
	Path         string
	Bind         string
	enableSingle bool
	localID      string

	mu   sync.RWMutex
	m    map[string]string // The key-value store for the system.
	set  *item.SortedSet
	sets map[string]*item.SortedSet

	raft      *raft.Raft // The consensus mechanism
	snapshots raft.SnapshotStore
	transport *RESPTransport
	trans     raft.Transport
	store     bigStore

	applier Applier

	observerCh chan raft.Observation
	observer   *raft.Observer
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

// New returns a new Slice.
func NewSlice(id int, enableSingle bool, path, localID string, applier Applier) *Slice {
	s := &Slice{
		id:           id,
		Path:         path,
		Bind:         localID,
		localID:      localID,
		enableSingle: enableSingle,
		applier:      applier,
		m:            make(map[string]string),
		sets:         make(map[string]*item.SortedSet),
		observerCh:   make(chan raft.Observation),
	}
	var name string
	if s.id < 0 {
		name = "clusterdb"
	} else {
		name = fmt.Sprintf("shard-%d", s.id)
	}

	s.BaseService = *service.NewBaseService(sliced.Logger, name, s)
	return s
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the master.
// localID should be the server identifier for this node.
func (s *Slice) OnStart() error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.localID)
	raftLogger := &raftLoggerWriter{
		logger: s.Logger,
	}
	config.Logger = log.New(raftLogger, "", 0)
	var err error

	// Open database
	var dbpath string
	if s.Path == ":memory:" {
		dbpath = ":memory:"
	} else {
		dbpath = ":memory:"
		//dbpath = filepath.Join(s.Path, "data.db")
	}
	_ = dbpath

	// Create Transport
	if s.Bind == "" {

	}
	//if s.Path == ":memory:" {
	//	addr, t := raft.NewInmemTransport("")
	//	s.trans = t
	//	_ = addr
	//	s.Logger.Info().Msgf("Transport: %s", addr)
	//} else {
	//s.transport = raft.NewTCPTransport(":16000", )
	s.transport = NewRaftTransport(s)
	s.trans = s.transport

	err = s.transport.Start()
	if err != nil {
		s.Logger.Error().Err(err).Msg("raft transport start failed")
		return err
	}
	//}

	// Create Snapshot Store
	if s.Path == ":memory:" {
		s.snapshots = raft.NewInmemSnapshotStore()
	} else {
		// Create the snapshot store. This allows the Raft to truncate the log.
		s.snapshots, err = raft.NewFileSnapshotStore(s.Path, retainSnapshotCount, raftLogger)
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
		logpath = ":memory:"
		//logpath = filepath.Join(s.Path, "raft.db")
	}
	var logname string
	if s.id < 0 {
		logname = "master"
	} else {
		logname = fmt.Sprintf("shard-%d-log", s.id)
	}
	// Create the log store and stable store.
	s.store, err = raftfastlog.NewFastLogStore(
		logpath,
		raftfastlog.Low,
		s.Logger.With().Str("component", logname).Logger(),
	)
	if err != nil {
		s.transport.Stop()
		s.Logger.Error().Err(err).Msg("log store failed")
		return fmt.Errorf("new log store: %s", err)
	}

	s.set = item.NewSortedSet()
	bootstrap := s.set.Length() == 0
	if bootstrap {
		//config.StartAsLeader = true
	}

	// Instantiate the Raft systems.
	s.raft, err = raft.NewRaft(config, (*fsm)(s), s.store, s.store, s.snapshots, s.trans)
	if err != nil {
		s.transport.Stop()
		s.store.Close()
		s.Logger.Error().Err(err).Msg("new raft failed")
		return fmt.Errorf("new raft: %s", err)
	}

	s.observer = raft.NewObserver(s.observerCh, false, func(o *raft.Observation) bool {
		return true
	})
	go s.runObserver()
	s.raft.RegisterObserver(s.observer)

	if bootstrap {
		//config.StartAsLeader = true
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: s.trans.LocalAddr(),
				},
			},
		}
		s.raft.BootstrapCluster(configuration)
	}

	return nil
}

func (s *Slice) OnStop() {
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

	close(s.observerCh)
}

func (s *Slice) GetSet() *item.SortedSet {
	s.mu.RLock()
	set := s.set
	s.mu.RUnlock()
	return set
}

func (s *Slice) RunSet(key string, fn func(set *item.SortedSet)) {
	s.mu.RLock()
	fn(s.set)
	s.mu.RUnlock()
}

type raftLoggerWriter struct {
	logger zerolog.Logger
}

func (w *raftLoggerWriter) Write(buf []byte) (int, error) {
	l := len(buf)
	b := buf
	lidx := bytes.IndexByte(b, '[')
	if lidx > -1 {
		b = b[lidx+1:]
		idx := bytes.IndexByte(b, ']')
		if idx > 0 {
			level := string(b[0:idx])

			b = b[idx+1:]
			name := "raft"
			idx = bytes.IndexByte(b, ':')

			if idx > 0 {
				name = string(bytes.TrimSpace(b[:idx]))
				b = b[idx+1:]
			}

			msg := strings.TrimSpace(string(b))
			switch level {
			case "WARN":
				w.logger.Warn().Str("component", name).Msg(msg)
			case "DEBU":
				w.logger.Debug().Str("component", name).Msg(msg)
			case "DEBUG":
				w.logger.Debug().Str("component", name).Msg(msg)
			case "INFO":
				w.logger.Info().Str("component", name).Msg(msg)
			case "ERR":
				w.logger.Error().Str("component", name).Msg(msg)
			case "ERRO":
				w.logger.Error().Str("component", name).Msg(msg)
			case "ERROR":
				w.logger.Error().Str("component", name).Msg(msg)

			default:
				w.logger.Info().Str("component", name).Msg(msg)
			}
		} else {
			w.logger.Info().Str("component", "raft").Msg(strings.TrimSpace(string(buf)))
		}
	} else {
		w.logger.Info().Str("component", "raft").Msg(strings.TrimSpace(string(buf)))
	}
	return l, nil
}

func (s *Slice) runObserver() {
	for {
		select {
		case observation, ok := <-s.observerCh:
			if !ok {
				return
			}
			str := ""
			data, err := json.Marshal(observation.Data)
			if err != nil {
				str = string(data)
			} else {
				str = fmt.Sprintf("%s", data)
			}
			s.Logger.Debug().Msgf("raft observation: %s", str)
		}
	}
}

func (s *Slice) Snapshot() error {
	f := s.raft.Snapshot()
	if err := f.Error(); err != nil {
		return err
	}
	return nil
}

// "SHRINK" client command.
func (c *Slice) Shrink() error {
	return ErrLogNotShrinkable
}

// "RAFTSHRINK" client command.
func (s *Slice) ShrinkLog() error {
	if s, ok := s.store.(shrinkable); ok {
		err := s.Shrink()
		if err != nil {
			return err
		}
		return nil
	}
	return ErrLogNotShrinkable
}

// Only for RESPTransport RAFTAPPEND
func (s *Slice) AppendEntries(o []byte, args [][]byte) ([]byte, error) {
	return s.transport.handleAppendEntries(o, args)
}

// Only for RESPTransport RAFTVOTE
func (s *Slice) RequestVote(o []byte, args [][]byte) ([]byte, error) {
	return s.transport.handleRequestVote(o, args)
}

// Only for RESPTransport RAFTINSTALL
func (s *Slice) HandleInstallSnapshot(conn *cmd.Context, arg []byte) cmd.Command {
	return s.transport.HandleInstallSnapshot(conn, arg)
}

func (s *Slice) Leader() (string, error) {
	return string(s.raft.Leader()), nil
}

func (s *Slice) Stats() map[string]string {
	return s.raft.Stats()
}

func (s *Slice) State() raft.RaftState {
	return s.raft.State()
}

func (s *Slice) Update(cmd string) (interface{}, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}
	return nil, nil
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Slice) Join(nodeID, addr string) error {
	s.Logger.Info().Str("node", nodeID).Str("addr", addr).Msg("node join request")

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		s.Logger.Info().Str("node", nodeID).Str("addr", addr).Err(f.Error()).Msg("node join failed")
		return f.Error()
	}
	s.Logger.Info().Str("node", nodeID).Str("addr", addr).Msg("node joined successfully")
	return nil
}

func (s *Slice) Leave(nodeID, addr string) error {
	s.Logger.Info().Str("node", nodeID).Str("addr", addr).Msg("node leave request")

	f := s.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	if f.Error() != nil {
		s.Logger.Info().Str("node", nodeID).Str("addr", addr).Err(f.Error()).Msg("node leave failed")
		return f.Error()
	}
	s.Logger.Info().Str("node", nodeID).Str("addr", addr).Msg("node left successfully")
	return nil
}

type fsm Slice

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	return f.applier.Apply((*Slice)(f), l)
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return &fsmSnapshot{}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// SortedSet the state from the snapshot, no lock required according to
	// Hashicorp docs.
	//f.db = db

	return nil
}

type fsmSnapshot struct {
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		//err := f.db.Save(sink)
		//if err != nil {
		//	return err
		//}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
