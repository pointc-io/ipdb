package aof

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// An error indicating a given key does not exist
var ErrKeyNotFound = errors.New("not found")
var ErrShrinking = errors.New("shrink in progress")

const minShrinkSize = 64 * 1024 * 1024

const (
	cmdSet         = '(' // Key+Val
	cmdDel         = ')' // Key
	cmdStoreLogs   = '[' // Count+Log,Log...  Log: Idx+Term+Type+Data
	cmdDeleteRange = ']' // Min+Max

	maxDataSize = math.MaxInt32
)

type LogStore struct {
	mu     sync.RWMutex
	aof    *AOF
	closed bool
	kvm    map[string][]byte
	lvm    map[uint64]*raft.Log

	buf []byte

	limits   bool
	min, max uint64
	flushMax uint64

	log zerolog.Logger

	// snapshot
	// compaction
}

func NewLogStore(path string) (*LogStore, error) {
	b := &LogStore{
		kvm: make(map[string][]byte),
		lvm: make(map[uint64]*raft.Log),
	}

	aof, err := Open(path, regionSizeMin, b.opener)
	if err != nil {
		return nil, err
	}

	b.aof = aof
}

func (b *LogStore) writeBuf() error {
	if _, err := b.aof.Write(b.buf); err != nil {
		return err
	}
	return nil
}

func bufferLog(buf []byte, log *raft.Log) []byte {
	var num = make([]byte, 8)
	binary.PutUvarint(num, log.Index)
	//binary.LittleEndian.PutUint64(num, log.Index)
	buf = append(buf, num...)
	binary.PutUvarint(num, log.Term)
	//binary.LittleEndian.PutUint64(num, log.Term)
	buf = append(buf, num...)
	buf = append(buf, byte(log.Type))
	binary.LittleEndian.PutUint64(num, uint64(len(log.Data)))
	buf = append(buf, num...)
	buf = append(buf, log.Data...)
	return buf
}

func (b *LogStore) fillLimits() {
	b.min, b.max = 0, 0
	for idx := range b.lvm {
		if b.min == 0 {
			b.min, b.max = idx, idx
		} else if idx < b.min {
			b.min = idx
		} else if idx > b.max {
			b.max = idx
		}
	}
	b.limits = true
}

// FirstIndex returns the first index written. 0 for no entries.
func (b *LogStore) FirstIndex() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	if b.limits {
		return b.min, nil
	}
	b.fillLimits()
	return b.min, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (b *LogStore) LastIndex() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	if b.limits {
		return b.max, nil
	}
	b.fillLimits()
	return b.max, nil
}

// GetLog gets a log entry at a given index.
func (b *LogStore) GetLog(idx uint64, log *raft.Log) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrClosed
	}
	vlog := b.lvm[idx]
	if vlog == nil {
		return raft.ErrLogNotFound
	}
	*log = *vlog
	return nil
}

// StoreLog stores a log entry.
func (b *LogStore) StoreLog(log *raft.Log) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}

	var num = make([]byte, 8)
	b.buf = b.buf[:0]
	b.buf = append(b.buf, cmdStoreLogs)
	binary.PutUvarint(num, uint64(1))
	//binary.LittleEndian.PutUint64(num, uint64(1))
	b.buf = append(b.buf, num...)
	b.buf = bufferLog(b.buf, log)
	if err := b.writeBuf(); err != nil {
		return err
	}

	b.lvm[log.Index] = log
	if b.limits {
		if b.min == 0 {
			b.min, b.max = log.Index, log.Index
		} else if log.Index < b.min {
			b.min = log.Index
		} else if log.Index > b.max {
			b.max = log.Index
		}
	}
	return nil
}

// StoreLogs stores multiple log entries.
func (b *LogStore) StoreLogs(logs []*raft.Log) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}

	var num = make([]byte, 10)
	b.buf = b.buf[:0]
	b.buf = append(b.buf, cmdStoreLogs)
	binary.PutUvarint(num, uint64(len(logs)))
	//binary.LittleEndian.PutUint64(num, uint64(len(logs)))
	b.buf = append(b.buf, num...)
	for _, log := range logs {
		b.buf = bufferLog(b.buf, log)
	}
	if err := b.writeBuf(); err != nil {
		return err
	}

	for _, log := range logs {
		b.lvm[log.Index] = log
		if b.limits {
			if b.min == 0 {
				b.min, b.max = log.Index, log.Index
			} else if log.Index < b.min {
				b.min = log.Index
			} else if log.Index > b.max {
				b.max = log.Index
			}
		}
	}
	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (b *LogStore) DeleteRange(min, max uint64) error {
	//start := time.Now()
	defer func() {
		//b.log.Info().
		//	Dur("duration", time.Now().Sub(start)).
		//	Uint64("min", min).
		//	Uint64("max", max).
		//	Msg("delete range completed")
	}()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}

	var num = make([]byte, 10)
	b.buf = b.buf[:0]
	b.buf = append(b.buf, cmdDeleteRange)
	binary.PutUvarint(num, uint64(min))
	//binary.LittleEndian.PutUint64(num, min)
	b.buf = append(b.buf, num...)
	binary.PutUvarint(num, uint64(max))
	//binary.LittleEndian.PutUint64(num, max)
	b.buf = append(b.buf, num...)
	if err := b.writeBuf(); err != nil {
		return err
	}

	for i := min; i <= max; i++ {
		delete(b.lvm, i)
	}
	b.limits = false
	return nil
}

func bufferSet(buf []byte, k, v []byte) []byte {
	var num = make([]byte, 10)
	buf = append(buf, cmdSet)
	binary.PutUvarint(num, uint64(len(k)))
	//binary.LittleEndian.PutUint64(num, uint64(len(k)))
	buf = append(buf, num...)
	buf = append(buf, k...)
	binary.PutUvarint(num, uint64(len(v)))
	//binary.LittleEndian.PutUint64(num, uint64(len(v)))
	buf = append(buf, num...)
	buf = append(buf, v...)
	return buf
}

// Set is used to set a key/value set outside of the raft log
func (b *LogStore) Set(k, v []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}

	b.buf = b.buf[:0]
	b.buf = bufferSet(b.buf, k, v)
	if err := b.writeBuf(); err != nil {
		return err
	}

	b.kvm[string(k)] = v
	return nil
}

// Get is used to retrieve a value from the k/v store by key
func (b *LogStore) Get(k []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrClosed
	}
	if val, ok := b.kvm[string(k)]; ok {
		return val, nil
	}
	return nil, ErrKeyNotFound
}

// SetUint64 is like Set, but handles uint64 values
func (b *LogStore) SetUint64(key []byte, val uint64) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, val)
	return b.Set(key, data)
}

// GetUint64 is like Get, but handles uint64 values
func (b *LogStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	if len(val) != 8 {
		return 0, errors.New("invalid number")
	}
	return binary.LittleEndian.Uint64(val), nil
}

// Compacted Snapshot
// Log

/*
Snapshot Process

 */
func (b *LogStore) opener(file *os.File) (int64, error) {
	var err error

	info, err := file.Stat()
	if err != nil {
		return 0, err
	}

	if info.Size() == 0 {
		return 0, nil
	}

	// Create a 4mb max buffer
	var bsize int
	if info.Size() < 4*1024*1024 {
		bsize = int(info.Size())
	} else {
		bsize = 4 * 1024 * 1024
	}

	var size = int64(0)

	_, err = file.Seek(0, 0)
	if err != nil {
		return 0, err
	}

	// Create a new reader.
	reader := bufio.NewReaderSize(file, bsize)

	var num uint64
	var count int
	var s int
	var n int
	var mark int64
	var t byte
	for {
		mark = size
		kind, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return mark, nil
			}
			return mark, err
		}

		switch kind {
		default:
			return mark, nil

		case cmdSet:
			// Read key
			num, s, err = readUvarint(reader)
			if err != nil {
				return mark, nil
			}
			key := make([]byte, int(num))
			n, err = reader.Read(key)
			if err != nil || n != s {
				return mark, nil
			}
			size += int64(s + int(num))

			// Read value
			num, s, err = readUvarint(reader)
			if err != nil {
				return mark, nil
			}
			val := make([]byte, int(num))
			n, err = reader.Read(val)
			if err != nil || n != s {
				return mark, nil
			}
			size += int64(s + int(num))

			// Ensure it's set
			b.kvm[string(key)] = val

		case cmdDel:
			// Key size.
			num, s, err = readUvarint(reader)
			if err != nil || num < 0 || num > maxDataSize {
				return mark, nil
			}
			key := make([]byte, int(num))
			n, err = reader.Read(key)
			if err != nil || n != s {
				return mark, nil
			}
			size += int64(s + int(num))
			// Ensure it's deleted
			delete(b.kvm, string(key))

		case cmdStoreLogs:
			num, s, err = readUvarint(reader)
			if err != nil {
				return mark, nil
			}

			count = int(num)
			for i := 0; i < count; i++ {
				var log raft.Log
				// Read log.Index
				log.Index, s, err = readUvarint(reader)
				if err != nil {
					return mark, nil
				}
				size += int64(s)

				// Read log.Term
				log.Term, s, err = readUvarint(reader)
				if err != nil {
					return mark, nil
				}
				size += int64(s)

				// Read log.Type
				t, err = reader.ReadByte()
				if err != nil {
					return mark, nil
				}
				size += 1
				log.Type = raft.LogType(t)

				// Read len(log.Data)
				num, s, err = readUvarint(reader)
				if err != nil {
					return mark, nil
				}
				if num > maxDataSize {
					return mark, nil
				}
				size += int64(s)

				// Read log.Data
				data := make([]byte, int(num))
				s, err = reader.Read(data)
				if err != nil {
					return mark, nil
				}
				if s != len(data) {
					return mark, nil
				}
				size += int64(num)
				log.Data = data

				// Add log entry
				b.lvm[log.Index] = &log

				// Calculate limits
				if b.limits {
					if b.min == 0 {
						b.min, b.max = log.Index, log.Index
					} else if log.Index < b.min {
						b.min = log.Index
					} else if log.Index > b.max {
						b.max = log.Index
					}
				}

				// Record is valid and accepted
				mark = size
			}

		case cmdDeleteRange:
			var min, max uint64

			min, s, err = readUvarint(reader)
			if err != nil {
				return mark, nil
			}
			size += int64(s)

			max, s, err = readUvarint(reader)
			if err != nil {
				return mark, nil
			}
			size += int64(s)

			for i := min; i < max; i++ {
				delete(b.lvm, i)
			}
			b.limits = false
		}
	}

	// Seek back to the beginning
	_, err = file.Seek(0, 1)
	if err != nil {
		return mark, err
	}

	return mark, nil
}
