package server

import (
	"sync"
	"time"
	"fmt"
	"net"
	"runtime"

	"github.com/rcrowley/go-metrics"

	"github.com/pointc-io/ipdb"
	"github.com/pointc-io/ipdb/redcon"
	"github.com/pointc-io/ipdb/evio"
	"github.com/pointc-io/ipdb/service"
	"strings"
	"github.com/pointc-io/ipdb/command"
)

var notImplementedHandler = func(out []byte, action evio.Action, conn *Conn, packet []byte, args [][]byte) ([]byte, evio.Action) {
	out = redcon.AppendError(out, "ERR not implemented")
	return out, action
}

//
type EvServer struct {
	service.BaseService

	// static values
	host      string
	http      bool
	dir       string
	started   time.Time
	maxMemory uint64

	// Net listener
	listener net.Listener

	// Metrics
	statsTotalConns    metrics.Counter // counter for total connections
	statsTotalCommands metrics.Counter // counter for total commands
	bytesIn            metrics.Counter
	bytesOut           metrics.Counter

	// Event loops
	loops  []*EvLoop
	action evio.Action

	// Mutex
	wg sync.WaitGroup
	mu sync.RWMutex

	// handler
	handler command.Handler
}

func NewEvServer(host string, eventLoops int) *EvServer {
	if eventLoops < 1 {
		eventLoops = 1
	}
	if eventLoops > runtime.NumCPU()*2 {
		eventLoops = runtime.NumCPU() * 2
	}
	s := &EvServer{
		host: host,

		statsTotalConns:    metrics.NewCounter(),
		statsTotalCommands: metrics.NewCounter(),
		bytesIn:            metrics.NewCounter(),
		bytesOut:           metrics.NewCounter(),

		wg: sync.WaitGroup{},
		mu: sync.RWMutex{},

		loops: make([]*EvLoop, eventLoops),
	}

	s.BaseService = *service.NewBaseService(sliced.Logger, "evserver", s)
	return s
}

func (s *EvServer) SetHandler(handler command.Handler) {
	s.handler = handler
}

func (s *EvServer) OnStart() error {
	addr, err := net.ResolveTCPAddr("tcp", s.host)
	if err != nil {
		return err
	}
	s.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	for i := 0; i < len(s.loops); i++ {
		s.loops[i] = NewEventLoop(i, s)
		err := s.loops[i].Start()
		if err != nil {
			s.Logger.Error().Err(err)

			for a := 0; a < i; a++ {
				s.loops[a].Stop()
			}

			return err
		}
	}

	s.Logger.Info().Str("host", s.host).Msg("listening")

	return nil
}

func (s *EvServer) OnStop() {
	s.action = evio.Shutdown
	for _, loop := range s.loops {
		err := loop.Stop()
		if err != nil {
			s.Logger.Error().Err(err)
		}
	}

	for _, loop := range s.loops {
		loop.Wait()
	}
	// Close the listener.
	s.listener.Close()
}

type EvLoop struct {
	service.BaseService

	id   int
	Host *EvServer
	Ev   *evio.Server

	conns map[int]*Conn

	totalConns    metrics.Counter // counter for total connections
	totalCommands metrics.Counter // counter for total commands
	totalBytesIn  metrics.Counter
	totalBytesOut metrics.Counter

	wg sync.WaitGroup
}

func NewEventLoop(id int, server *EvServer) *EvLoop {
	e := &EvLoop{
		id:    id,
		Host:  server,
		wg:    sync.WaitGroup{},
		conns: make(map[int]*Conn),

		totalConns:    metrics.NewCounter(),
		totalCommands: metrics.NewCounter(),
		totalBytesIn:  metrics.NewCounter(),
		totalBytesOut: metrics.NewCounter(),
	}
	e.BaseService = *service.NewBaseService(sliced.Logger, fmt.Sprintf("evloop-%d", id), e)
	return e
}

func (e *EvLoop) OnStart() error {
	e.wg.Add(1)
	go e.serve()
	return nil
}

func (e *EvLoop) OnStop() {
	e.Ev.Shutdown()
}

func (e *EvLoop) Wait() {
	e.wg.Wait()
}

func (e *EvLoop) serve() {
	defer e.wg.Done()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	conns := e.conns

	var events evio.Events
	var ev evio.Server
	s := e.Host

	events.Serving = func(loop evio.Server) (action evio.Action) {
		ev = loop
		e.Ev = &ev
		return
	}
	events.Opened = func(id int, info evio.Info) (out []byte, opts evio.Options, action evio.Action) {
		// Create new Conn
		c := &Conn{
			id:      id,
			ev:      e,
			handler: s.handler,
		}
		conns[id] = c
		c.durability = command.Medium
		return
	}
	events.Tick = func() (delay time.Duration, action evio.Action) {
		// Tick every second.
		delay = time.Second
		if s.action == evio.Shutdown {
			action = evio.Shutdown
		}
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
	//events.Postwrite = func(id int, amount, remaining int) (action evio.Action) {
	//	s.bytesOut.Inc(int64(amount))
	//	return
	//}
	events.Detached = func(id int, conn net.Conn) (action evio.Action) {
		c, ok := conns[id]
		if !ok {
			return
		}
		delete(conns, id)
		_ = c
		return
	}
	events.Data = func(id int, in []byte) (out []byte, action evio.Action) {
		c, ok := conns[id]
		if !ok {
			if id == -1 {
				s.Logger.Error().Msg("shutdown action")
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

		//s.bytesIn.Inc(int64(len(in)))

		// Does the connection have some news to tell the event loop?
		if c.evaction != evio.None {
			action = c.evaction
			return
		}

		c.statsTotalUpstream += uint64(len(in))

		// A single buffer is reused at the eventloop level.
		// If we get partial commands then we need to copy to
		// an allocated buffer at the connection level.
		// Zero copy if possible strategy.
		data := c.begin(in)

		//var packet []byte
		var complete bool
		var err error
		//var args [][]byte
		var cmd command.Command
		var cmdCount = 0

		ctx := &command.Context{
			Conn: c,
			Out:  out,
		}

		for action == evio.None {
			// Read next command.
			ctx.Packet, complete, ctx.Args, _, data, err = redcon.ParseNextCommand(data, ctx.Args[:0])

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

			numArgs := len(ctx.Args)
			if numArgs > 0 {
				c.statsTotalCommands++
				cmdCount++

				ctx.Name = strings.ToUpper(string(ctx.Args[0]))

				if numArgs > 1 {
					ctx.Key = string(ctx.Args[1])
					//ctx.Key = *(*string)(unsafe.Pointer(&ctx.Args[1]))
				} else {
					ctx.Key = ""
				}

				// Parse Command
				cmd = c.handler.Parse(ctx)

				if cmd == nil {
					cmd = command.ERR(fmt.Sprintf("ERR command '%s' not found", ctx.Name))
				}

				if cmd.IsWrite() {
					ctx.AddChange(cmd, ctx.Packet)
				} else {
					// Commit if necessary
					ctx.Commit()

					// Append directly.
					ctx.Out = cmd.Invoke(ctx)
				}

				ctx.Index++
			}
		}

		// Copy partial Cmd data if any.
		c.end(data)

		c.statsTotalDownstream += uint64(len(out))

		if action == evio.Close {
			return
		}

		if cmd == nil {
			return ctx.Out, c.evaction
		}

		// Flush commit buffer
		ctx.Commit()

		return ctx.Out, c.evaction
	}

	err := evio.ServeListener(events, false, s.listener)
	//err := evio.Serve(events, addrs...)
	if err != nil {
		e.Logger.Error().Err(err)
	}
}

func (e *EvLoop) Wake(id int) {
	e.Ev.Wake(id)
}
