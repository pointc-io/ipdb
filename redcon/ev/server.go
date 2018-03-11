package evred

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
	"github.com/pointc-io/ipdb/db/buntdb"
)

var notImplementedHandler = func(out []byte, action evio.Action, conn *Conn, packet []byte, args [][]byte) ([]byte, evio.Action) {
	out = redcon.AppendError(out, "ERR not implemented")
	return out, action
}

type Handler interface {
	ShardID(key string) int

	Parse(ctx *CommandContext) Command

	//NextCommand(out []byte, action evio.Action, conn *Conn, packet []byte, args [][]byte) ([]byte, evio.Action)

	Commit(ctx *CommandContext)
}

type ApplyMode int

const (
	Medium ApplyMode = iota
	High   ApplyMode = 1
)

type CommitLog interface {
}

//
type Server struct {
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

	// Handler
	handler Handler
}

func NewServer(host string, eventLoops int) *Server {
	if eventLoops < 1 {
		eventLoops = 1
	}
	if eventLoops > runtime.NumCPU()*2 {
		eventLoops = runtime.NumCPU() * 2
	}
	s := &Server{
		host: host,

		statsTotalConns:    metrics.NewCounter(),
		statsTotalCommands: metrics.NewCounter(),
		bytesIn:            metrics.NewCounter(),
		bytesOut:           metrics.NewCounter(),

		wg: sync.WaitGroup{},
		mu: sync.RWMutex{},

		loops: make([]*EvLoop, eventLoops),
	}

	s.BaseService = *service.NewBaseService(butterd.Logger, "evserver", s)
	return s
}

func (s *Server) SetHandler(handler Handler) {
	s.handler = handler
}

func (s *Server) OnStart() error {
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

func (s *Server) OnStop() {
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
	Host *Server
	Ev   *evio.Server

	conns map[int]*Conn

	totalConns    metrics.Counter // counter for total connections
	totalCommands metrics.Counter // counter for total commands
	totalBytesIn  metrics.Counter
	totalBytesOut metrics.Counter

	wg sync.WaitGroup
}

func NewEventLoop(id int, server *Server) *EvLoop {
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
	e.BaseService = *service.NewBaseService(butterd.Logger, fmt.Sprintf("evloop-%d", id), e)
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
		c.Consistency = Medium
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
		var cmd Command
		var cmdCount = 0

		ctx := &CommandContext{
			Conn: c,
			Out:  out,
		}

		for action == evio.None {
			// Read next command.
			//packet, complete, args, _, data, err = redcon.ReadNextCommand2(data, args[:0])
			ctx.Packet, complete, ctx.Args, _, data, err = redcon.ReadNextCommand2(data, ctx.Args[:0])

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
					cmd = ERR(fmt.Sprintf("ERR command '%s' not found", ctx.Name))
				}

				if cmd.IsWrite() {
					ctx.AddChange(cmd, ctx.Packet)
				} else {
					// Commit if necessary
					ctx.Commit()

					// Append directly.
					ctx.Out = cmd.Invoke(ctx)
				}

				//if !dispatched {
				//	before := len(out)
				//	out = cmd.Invoke(out)
				//
				//	// Do we need to dispatch on Worker pool?
				//	if len(out) == before {
				//		dispatched = true
				//		c.dispatch(cmd)
				//	} else {
				//		commands = append(commands, cmd)
				//
				//		if cmd.IsWrite() {
				//			// Add to commit log.
				//			c.commitlog = append(c.commitlog, cmd)
				//			c.commitbuf = cmd.Commit(c.commitbuf, packet)
				//		} else {
				//		}
				//	}
				//} else {
				//	// Append onto connection backlog
				//	c.backlog = append(c.backlog, cmd)
				//}
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

type CommandContext struct {
	Conn    *Conn // Connection
	Out     []byte
	Index   int // Index of command
	Name    string
	Key     string // Key if it exists
	ShardID int    // Shard ID if calculated
	Packet  []byte
	Args    [][]byte
	Tx      *buntdb.Tx
	DB      *buntdb.DB

	Changes map[int]*ChangeSet
}

type ChangeSet struct {
	Cmds []Command
	Data []byte
}

func (c *CommandContext) AppendOK() []byte {
	return redcon.AppendOK(c.Out)
}

func (c *CommandContext) AppendNull() []byte {
	return redcon.AppendNull(c.Out)
}

func (c *CommandContext) AppendError(msg string) []byte {
	return redcon.AppendError(c.Out, msg)
}

func (c *CommandContext) AppendBulkString(bulk string) []byte {
	return redcon.AppendBulkString(c.Out, bulk)
}

func (c *CommandContext) ApppendBulk(bulk []byte) []byte {
	return redcon.AppendBulk(c.Out, bulk)
}

func (c *CommandContext) ApppendInt(val int) []byte {
	return redcon.AppendInt(c.Out, int64(val))
}

func (c *CommandContext) ApppendInt64(val int64) []byte {
	return redcon.AppendInt(c.Out, val)
}

func (c *CommandContext) Commit() {
	if c.HasChanges() {
		c.Conn.handler.Commit(c)
	}
}

func (c *CommandContext) HasChanges() bool {
	return len(c.Changes) > 0
}

func (c *CommandContext) AddChange(cmd Command, data []byte) {
	if c.Changes == nil {
		c.Changes = map[int]*ChangeSet{
			c.ShardID: {
				Cmds: []Command{cmd},
				Data: data,
			},
		}
	} else {
		set, ok := c.Changes[c.ShardID]
		if !ok {
			c.Changes[c.ShardID] = &ChangeSet{
				Cmds: []Command{cmd},
				Data: c.Packet,
			}
		} else {
			set.Cmds = append(set.Cmds, cmd)
			set.Data = append(set.Data, c.Packet...)
		}
	}
}
