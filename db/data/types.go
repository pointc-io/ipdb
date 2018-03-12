package data

import (
	"github.com/pointc-io/ipdb/redcon/ev"
	"github.com/pointc-io/ipdb/service"
	"io"
	"sync"
)

type StructureType uint8

const (
	Blob   StructureType = 0 // Arbitrary string
	Object StructureType = 1 // Arbitrary JSON object
	Number StructureType = 2 // Integer type supports INCR, INCRBY
	List   StructureType = 3 // Indexed list of another Structure
	//Set         StructureType = 4 // Hash
	SortedSet   StructureType = 5 // BTree Set with primary key
	HyperLogLog StructureType = 6 // HyperLogLog++ compressed 8.2kb
	Stream      StructureType = 7 // Stream like structure e.g. Kafka, Kinesis
	PubSub      StructureType = 8 // Publish / Subscriber type messaging
)

type Structure interface {
	// Current committed log index
	Index() uint64

	Type() StructureType

	// Handle command
	Process(ctx *evred.CommandContext) evred.Command

	// Snapshot RESP to AOF
	Snapshot(writer io.Writer) error

	// Restore from RESP AOF
	Restore(reader io.Reader) error
}

type DataService struct {
	service.BaseService

	// Root map that contains all root level keys
	data map[string]Structure

	// Global mutex
	mu sync.RWMutex
}

func (s *DataService) OnStart() error {
	return nil
}

func (s *DataService) OnStop() {
}

// Safely retrieves a structure
func (s *DataService) Get(key string) Structure {
	s.mu.RLock()
	structure, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return nil
	} else {
		return structure
	}
}

func (s *DataService) View(fn func()) error {
	s.mu.RLock()
	fn()
	s.mu.RUnlock()
	return nil
}

func (s *DataService) Update(fn func()) error {
	s.mu.Lock()
	fn()
	s.mu.Unlock()
	return nil
}
