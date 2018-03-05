package server

import (
	"sync"
	"time"
	"strings"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/pointc-io/ipdb/redcon"
	"github.com/pointc-io/ipdb/pool/pbufio"
	"github.com/pointc-io/ipdb/pool/pbytes"
	"github.com/pointc-io/ipdb/evio"
	"github.com/pointc-io/ipdb/service"

	"github.com/rcrowley/go-metrics"
	"github.com/rs/zerolog"
)

// Buffers
var ReaderPool = pbufio.DefaultReaderPool
var WriterPool = pbufio.DefaultWriterPool
var BufferPool = pbytes.DefaultPool
// Server
var Svr *Server

type Server struct {
	service.BaseService

	// static values
	host      string
	port      int
	http      bool
	dir       string
	started   time.Time
	maxMemory uint64

	logger zerolog.Logger

	statsTotalConns        metrics.Counter // counter for total connections
	statsTotalCommands     metrics.Counter // counter for total commands
	stopBackgroundExpiring abool
	stopWatchingMemory     abool
	stopWatchingAutoGC     abool
	outOfMemory            abool

	evloop *evio.Server
	conns  map[int]*dbConn
	wg     *sync.WaitGroup

	mu sync.RWMutex
}

func NewServer(logger zerolog.Logger, host string, port int) *Server {
	Svr = &Server{
		host:   host,
		port:   port,
		logger: logger,
		wg:     &sync.WaitGroup{},

		conns: make(map[int]*dbConn),
	}

	Svr.BaseService = *service.NewBaseService(logger, "Server", Svr)
	return Svr
}

func (s *Server) OnStart() error {
	s.wg.Add(1)
	go s.serve()
	return nil
}

func (s *Server) OnStop() {
	s.evloop.Wake(-1)
	s.wg.Wait()
}

func (s *Server) serve() {
	defer s.wg.Done()

	conns := Svr.conns

	var events evio.Events
	var evloop evio.Server

	events.Serving = func(loop evio.Server) (action evio.Action) {
		evloop = loop
		Svr.evloop = &loop
		s.logger.Info().Msgf("ipdb server started on port %d", s.port)
		return
	}
	events.Opened = func(id int, info evio.Info) (out []byte, opts evio.Options, action evio.Action) {
		c := &dbConn{
			id:     id,
			evloop: &evloop,
		}
		conns[id] = c
		return
	}
	events.Tick = func() (delay time.Duration, action evio.Action) {
		//now := time.Now()
		//for id, t := range wgetids {
		//	if now.Sub(t) > time.Second {
		//		Svr.srv.Wake(id)
		//	}
		//}
		// Tick every second.
		delay = time.Second
		return
	}
	events.Closed = func(id int, err error) (action evio.Action) {
		c, ok := conns[id]
		if !ok {
			return
		}
		delete(conns, id)

		// Notify connection.
		c.closed()

		return
	}
	events.Data = func(id int, in []byte) (out []byte, action evio.Action) {
		c, ok := conns[id]
		if !ok {
			if id == -1 {
				action = evio.Shutdown
			} else {
				action = evio.Close
			}
			return
		}

		// Is it a wake?
		if in == nil {
			return c.woke()
		}

		// Does the connection have some news to tell the event loop?
		if c.evaction != evio.None {
			action = c.evaction
			return
		}

		// A single buffer is reused at the eventloop level.
		// If we get partial commands then we need to copy to
		// an allocated buffer at the connection level.
		// Zero copy if possible strategy.
		data := c.is.Begin(in)

		var complete bool
		var err error
		var args [][]byte
		for action == evio.None {
			complete, args, _, data, err = redcon.ReadNextCommand(data, args[:0])

			if err != nil {
				action = evio.Close
				out = redcon.AppendError(out, err.Error())
				break
			}

			// Do we need more data?
			if !complete {
				// Exit loop.
				break
			}

			if len(args) > 0 {
				// First argument is the command string.
				r := c.incoming(strings.ToUpper(string(args[0])), args)
				if len(r) > 0 {
					out = append(out, r...)
				}
			}
		}

		// Copy partial command data if any.
		c.is.End(data)
		return
	}

	//go Svr.watchOutOfMemory()

	var ssuf string
	//if stdlib {
	//	ssuf = "-net"
	//}
	addrs := []string{fmt.Sprintf("tcp"+ssuf+"://:%d", s.port)}
	//if unixsocket != "" {
	//	addrs = append(addrs, fmt.Sprintf("unix"+ssuf+"://%s", unixsocket))
	//}
	err := evio.Serve(events, addrs...)
	if err != nil {
		log.Fatal(err)
	}
}

type aint struct{ v int64 }

func (a *aint) add(d int) int {
	return int(atomic.AddInt64(&a.v, int64(d)))
}
func (a *aint) get() int {
	return int(atomic.LoadInt64(&a.v))
}
func (a *aint) set(i int) int {
	return int(atomic.SwapInt64(&a.v, int64(i)))
}

type abool struct{ v int64 }

func (a *abool) on() bool {
	return atomic.LoadInt64(&a.v) != 0
}
func (a *abool) set(t bool) bool {
	if t {
		return atomic.SwapInt64(&a.v, 1) != 0
	}
	return atomic.SwapInt64(&a.v, 0) != 0
}

type astring struct {
	mu sync.Mutex
	v  string
}

func (a *astring) get() string {
	a.mu.Lock()
	p := a.v
	a.mu.Unlock()
	return p
}
func (a *astring) set(s string) string {
	a.mu.Lock()
	p := a.v
	a.v = s
	a.mu.Unlock()
	return p
}

type atime struct {
	mu sync.Mutex
	v  time.Time
}

func (a *atime) get() time.Time {
	a.mu.Lock()
	p := a.v
	a.mu.Unlock()
	return p
}
func (a *atime) set(t time.Time) time.Time {
	a.mu.Lock()
	p := a.v
	a.v = t
	a.mu.Unlock()
	return p
}
