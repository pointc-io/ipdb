package aof

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

var (
	ErrEOF = errors.New("EOF")
)

const (
	RegionSize int64 = 1024 * 1024 * 256 // 256mb
)

// Append-Only file where the tail is memory-mapped by a specified
// "region" size. fsync() happens in the background and once a region
// is filled up it is unmapped after it's been fsync'ed completely.
// Close() will wait until all regions are fsync'ed and subsequently
// unmapped and the goroutine exits.
//
// fsync() can be user initiated and is also called on a timer all
// in the background. New tail regions are also mapped in the background.
// The goal is to make writes blazing fast and limited by memcpy() speed.
// In reality it will be slower and depends on OS kernel.
type AOF struct {
	file   *os.File
	length int64
	//mmap   mmap.MMap

	maxMemory  int64
	regionSize int

	region       *region
	flushedIndex int
	regions      []*region

	// Position that is has been flushed
	flushPos int64

	ctx      context.Context
	cancel   context.CancelFunc
	ch       chan action
	chRegion chan *region

	// Sync
	mu sync.RWMutex
	wg sync.WaitGroup
}

//
func NewAOF(path string) (*AOF, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	_ = info

	ctx, cancel := context.WithCancel(context.Background())
	a := &AOF{
		file:     file,
		ctx:      ctx,
		cancel:   cancel,
		ch:       make(chan action, 1),
		chRegion: make(chan *region),
	}
	a.open()
	a.wg.Add(1)
	go a.run()
	return a, nil
}

func (a *AOF) open() error {
	info, err := a.file.Stat()
	if err != nil {
		return nil
	}

	// Create regions for existing file.
	for i, pos := 0, int64(0); pos < info.Size(); i, pos = i+1, pos+RegionSize {
		r := &region{
			index:     i,
			pos:       0,
			posDirty:  0,
			posFlush:  0,
			length:    0,
			offset:    0,
			offsetEnd: 0,
			state:     0,
			mmap:      nil,
			err:       nil,
		}

		a.regions = append(a.regions, r)
	}

	return nil
}

//
func (a *AOF) Append(buf []byte) (int64, int, error) {
	a.mu.Lock()

	remaining := a.region.length - a.region.pos
	if len(buf) > remaining {
		a.region.state = dirty
		copy(a.region.mmap, buf[:remaining])

	} else {
		a.region.state = dirty
		copy(a.region.mmap, buf)
		a.region.pos += len(buf)
	}
	a.mu.Unlock()

	return 0, 0, nil
}

//
func (a *AOF) mapNextRegion(offset int64) {
	mmap.MapRegion(a.file, a.regionSize, mmap.RDWR, 0, offset)
}

//
func (a *AOF) Flush() error {
	a.ch <- doFlush
	return nil
}

func (a *AOF) Close() error {
	a.wg.Wait()
	return nil
}

func (a *AOF) ReaderAt(pos int64) io.Reader {
	return nil
}

func (a *AOF) run() {
	defer a.wg.Done()

	//LOOP:
	for {
		select {
		case <-a.ctx.Done():
			a.flush()
			return

		case <-time.After(time.Second):
			// Should add region?

			// fsync()
			a.flush()

		case ac := <-a.ch:
			switch ac {
			case doFlush:
				a.flush()
			}
		}
	}
}

func (a *AOF) flush() {
	var regions []*region
	a.mu.Lock()
	for i := a.flushedIndex; i < len(a.regions); i++ {
		r := a.regions[i]
		if r.state != dirty {
			continue
		}
		// Set to flushing and mark position
		r.state = flushing
		r.posDirty = r.pos
		// Add region
		regions = append(regions, r)
	}
	a.mu.Unlock()

	for _, r := range regions {
		err := r.mmap.Flush()

		canClose := false
		a.mu.Lock()
		if err != nil {
			r.err = err
		} else {
			r.posFlush = r.posDirty
			r.posDirty = 0

			if r.pos == r.posFlush {
				r.state = flushed
				canClose = r.pos == r.length
			} else {
				r.state = dirty
			}
		}
		a.mu.Unlock()

		if canClose {
			err := r.mmap.Unmap()

			if err != nil {
				a.mu.Lock()
				r.err = err
				a.mu.Unlock()
			}
		}
	}
}

type action int

const (
	doFlush action = iota
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
type region struct {
	index     int
	pos       int
	posDirty  int
	posFlush  int
	length    int
	offset    int64
	offsetEnd int64
	state     regionState
	mmap      mmap.MMap
	err       error
}

func (r *region) Close() error {
	r.mmap.Flush()
	return r.mmap.Unmap()
}
