package aof

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

var (
	ErrEOF           = errors.New("EOF")
	ErrClosed        = errors.New("closed")
	ErrOverflow      = errors.New("overflow")
	ErrMmap          = errors.New("mmap failure")
	pageSize         = int64(os.Getpagesize())
	regionSize       = pageSize * pageSize
	regionSizeMin    = os.Getpagesize()
	truncateMultiple = int64(2)
)

type action int

const (
	flush  action = iota
	extend
)

//
type regionState int

const (
	flushed  regionState = iota
	flushing
	dirty
	closed
)

//
type Opener func(file *os.File) (int64, error)

// Append-Only file where the tail is memory-mapped by a specified
// "active" size. msync() happens in the background and once a active
// is filled up it is unmapped after it's been msync'ed completely.
// Close() will wait until all filled are msync'ed and subsequently
// unmapped and the background goroutine exits.
//
// msync() can be user initiated and is also called on a timer all
// in the background. New tail filled are also mapped in the background.
// The goal is to make writes blazing fast and limited by memcpy() speed.
type AOF struct {
	file   *os.File
	length int64 // logical length
	size   int64 // physical size on file-system

	// Position that is has been flushed
	flushPos int64
	extend   bool
	err      error

	regionSize int

	active *region   // active region
	next   *region   // next region
	filled []*region // filled regions awaiting munmap

	lbuf []byte

	// Context
	ctx    context.Context
	cancel context.CancelFunc

	// Channels
	closed   bool
	chAction chan action

	// Sync
	mu sync.RWMutex
	wg sync.WaitGroup
}

//
func Open(path string, regionSize int, opener Opener) (*AOF, error) {
	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0700)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size, err := opener(file)
	if err != nil && err != io.EOF {
		return nil, err
	}

	length := size

	if regionSize < regionSizeMin {
		regionSize = regionSizeMin
	}

	ctx, cancel := context.WithCancel(context.Background())
	a := &AOF{
		file:       file,
		size:       info.Size(),
		length:     length,
		ctx:        ctx,
		cancel:     cancel,
		regionSize: regionSize,
		chAction:   make(chan action, 1),
		lbuf:       make([]byte, 16),
		flushPos:   size,
	}

	var region *region
	if size > 0 {
		if size < int64(regionSize) {
			region, err = a.addRegion(0, regionSize)
			region.pos = int(size)
			region.posSnapshot = region.pos
			region.posFlush = region.pos
		} else {
			numRegions := size / int64(regionSize)
			offset := size % int64(regionSize)
			region, err = a.addRegion(numRegions*int64(regionSize), regionSize)
			region.pos = int(offset)
			region.posSnapshot = region.pos
			region.posFlush = region.pos
		}
	} else {
		region, err = a.addRegion(0, regionSize)
	}

	if err != nil {
		file.Close()
		return nil, err
	}

	a.active = region

	// Fill up the next regions
	a.fillNext()

	// Start goroutine
	a.wg.Add(1)
	go a.run()

	// Return AOF
	return a, nil
}

func (a *AOF) Write(buf []byte) (int, error) {
	a.mu.Lock()
	w, err := a.append(buf)
	a.mu.Unlock()

	if a.extend {
		a.chAction <- extend
	}
	return w, err
}

//
func (a *AOF) AppendWithLength(buf []byte) (int, error) {
	l := len(buf)
	if l <= 0 {
		return 0, nil
	}

	a.mu.Lock()
	r := binary.PutVarint(a.lbuf, int64(l))
	a.append(a.lbuf[:r])
	w, err := a.append(buf)
	a.mu.Unlock()

	if a.extend {
		a.chAction <- extend
	}
	return w, err
}

//
func (a *AOF) AppendEntry(header []byte, data []byte) (int, error) {
	l := len(header)
	if l <= 0 {
		return 0, nil
	}

	a.mu.Lock()

	// Append varint length
	r := binary.PutVarint(a.lbuf, int64(l)+int64(len(data)))
	a.append(a.lbuf[:r])
	w, err := a.append(header)
	if err != nil {
		a.mu.Unlock()
		return 0, err
	}
	w, err = a.append(data)
	if err != nil {
		a.mu.Unlock()
		return 0, err
	}
	a.mu.Unlock()

	if a.extend {
		a.chAction <- extend
	}
	return w, err
}

func (a *AOF) append(buf []byte) (int, error) {
	remaining := a.active.length - a.active.pos

	if len(buf) > remaining {
		copy(a.active.mmap[a.active.pos:], buf[:remaining])

		// Flush
		a.active.state = dirty
		a.active.pos = a.active.length
		a.length += int64(remaining)

		// Assign the next active
		err := a.nextRegion()
		if err != nil {
			return remaining, err
		}
		r, err := a.append(buf[remaining:])
		return r + remaining, err
	} else {
		copy(a.active.mmap[a.active.pos:], buf)

		// Flag as filled
		a.active.state = dirty
		a.active.pos += len(buf)
		a.length += int64(len(buf))

		if a.active.isFull() {
			// Assign the next active
			err := a.nextRegion()
			if err != nil {
			}
		}

		return len(buf), nil
	}
}

func (a *AOF) nextRegion() error {
	a.filled = append(a.filled, a.active)

	if a.next == nil {
		// Double active size
		a.regionSize *= 2

		a.active, a.err = a.addRegion(a.active.offsetEnd, a.regionSize)

		if a.err != nil {
			return a.err
		}
		if a.active == nil {
			return ErrMmap
		}

		a.extend = true
	} else {
		a.active = a.next
		a.next = nil
		a.extend = true
	}
	return nil
}

func (a *AOF) fillNext() error {
	nextOffset := int64(0)
	a.mu.Lock()
	if a.next == nil {
		nextOffset = a.active.offsetEnd
	}
	a.extend = false
	a.mu.Unlock()

	if nextOffset > 0 {
		next, err := a.addRegion(nextOffset, a.regionSize)
		if err != nil {
			return err
		}

		a.mu.Lock()
		if a.next == nil {
			// Was there a race and we lost?
			if a.active.offsetEnd > nextOffset {
				// Close newly mapped active
				next.close()
				// Map the new active
				next, err = a.addRegion(a.active.offsetEnd, a.regionSize)
				if err != nil {
					a.mu.Unlock()
					return err
				}
				a.next = next
			} else {
				a.next = next
			}
		} else {
			// Was there a race and we lost?
			next.close()
		}
		a.mu.Unlock()
	}

	return nil
}

//
func (a *AOF) Flush() error {
	a.chAction <- flush
	return nil
}

func (a *AOF) Close() error {
	a.mu.Lock()
	if !a.closed {
		close(a.chAction)
		a.cancel()
	}
	a.closed = true
	a.mu.Unlock()

	// Wait for goroutine to exit
	a.wg.Wait()
	return nil
}

func (a *AOF) run() {
	defer a.wg.Done()

	for {
		select {
		case <-a.ctx.Done():
			a.flush()
			return

		case <-time.After(time.Second):
			// msync()
			a.flush()

		case ac := <-a.chAction:
			switch ac {
			case flush:
				a.flush()
			case extend:
				a.fillNext()
			}
		}
	}
}

func (a *AOF) flush() {
	flushActive := false
	var filled []*region = nil

	a.mu.Lock()
	if a.active.posSnapshot < a.active.pos || a.closed {
		flushActive = true
		a.active.state = dirty
		a.active.posSnapshot = a.active.pos
	}
	if len(a.filled) > 0 {
		filled = make([]*region, len(a.filled))
		copy(filled, a.filled)
		a.filled = nil
	}
	a.mu.Unlock()

	if flushActive {
		a.active.mmap.Flush()
		a.mu.Lock()
		a.active.state = flushed
		a.active.posFlush = a.active.posSnapshot
		a.mu.Unlock()
	}

	// Close the filled regions.
	for _, region := range filled {
		region.close()
	}
}

func (a *AOF) addRegion(offset int64, length int) (*region, error) {
	// Do we need to extend file?
	newSize := offset + (int64(length) * truncateMultiple)
	if a.size <= newSize {
		err := a.file.Truncate(newSize)
		if err != nil {
			return nil, err
		}
		a.size = newSize
	}

	// mmap the active
	m, err := mmap.MapRegion(a.file, length, mmap.RDWR, 0, offset)
	if err != nil {
		return nil, err
	}
	return &region{
		pos:         0,
		posSnapshot: 0,
		posFlush:    0,
		length:      length,
		offset:      offset,
		offsetEnd:   offset + int64(length),
		state:       flushed,
		mmap:        m,
	}, nil
}

// mmap active of the file
type region struct {
	index       int
	pos         int
	posSnapshot int
	posFlush    int
	length      int
	offset      int64
	offsetEnd   int64
	state       regionState
	mmap        mmap.MMap
	err         error
}

func (r *region) isFull() bool {
	return r.pos == r.length
}

func (r *region) close() error {
	if r.posFlush < r.pos {
		r.mmap.Flush()
		r.posFlush = r.pos
	}
	defer func() {
		r.state = closed
	}()
	return r.mmap.Unmap()
}

func VarintLengthOpener(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}

	if info.Size() == 0 {
		return 0, nil
	}

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

	maxDiscard := int64(math.MaxInt32)
	var mark int64
	for {
		mark = size
		entrySize, bytesRead, err := readVarint(reader)

		if err != nil {
			if err == io.EOF {
				return mark, nil
			}
			return mark, err
		}

		if entrySize == 0 {
			return mark, nil
		}

		// Increase by size of size varint
		size += int64(bytesRead)

		for entrySize > 0 {
			var discard int
			if entrySize > maxDiscard {
				discard = int(maxDiscard)
			} else {
				discard = int(entrySize)
			}

			discarded, err := reader.Discard(discard)
			if err != nil {
				return mark, err
			}

			entrySize -= int64(discarded)
			size += int64(discarded)
		}
	}
}

// readUvarint reads an encoded unsigned integer from r and returns it as a uint64.
func readUvarint(r io.ByteReader) (uint64, int, error) {
	var x uint64
	var s uint
	for i := 0; ; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return x, i + 1, err
		}
		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return x, i + 1, ErrOverflow
			}
			return x | uint64(b)<<s, i + 1, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

// readVarint reads an encoded signed integer from r and returns it as an int64.
func readVarint(r io.ByteReader) (int64, int, error) {
	ux, size, err := readUvarint(r) // ok to continue in presence of error
	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}
	return x, size, err
}
