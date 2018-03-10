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
	"github.com/hashicorp/raft"
)

var notImplementedHandler = func(out []byte, action evio.Action, conn *Conn, packet []byte, args [][]byte) ([]byte, evio.Action) {
	out = redcon.AppendError(out, "ERR not implemented")
	return out, action
}

type Handler interface {
	ShardID(key string) int

	Parse(conn *Conn, packet []byte, args [][]byte) Command

	//NextCommand(out []byte, action evio.Action, conn *Conn, packet []byte, args [][]byte) ([]byte, evio.Action)

	Commit(conn *Conn, shard int, commitlog []byte) (raft.ApplyFuture, error)
}

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
			id: id,
			ev: e,
		}
		conns[id] = c
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
		c.onDetached(conn)
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

		// A single buffer is reused at the eventloop level.
		// If we get partial commands then we need to copy to
		// an allocated buffer at the connection level.
		// Zero copy if possible strategy.
		data := c.begin(in)

		var packet []byte
		var complete bool
		var err error
		var args [][]byte
		var cmd Command
		var cmdCount = 0

		var commitMap map[int]*struct {
			cmds []Command
			buf  []byte
		}

		flush := func() {
			for shardID, group := range commitMap {
				l := len(group.cmds)
				if l == 0 {
					continue
				}

				// Commit
				future, err := s.handler.Commit(c, shardID, group.buf)

				if err != nil {
					for i := 0; i < l; i++ {
						out = redcon.AppendError(out, "ERR "+err.Error())
					}
				} else if err = future.Error(); err != nil {
					for i := 0; i < l; i++ {
						out = redcon.AppendError(out, "ERR "+err.Error())
					}
				} else if resp := future.Response(); resp != nil {
					if buf, ok := resp.([]byte); ok {
						out = append(out, buf...)
					} else {
						switch t := resp.(type) {
						case string:
							out = redcon.AppendBulkString(out, t)
						default:
							for i := 0; i < l; i++ {
								out = redcon.AppendError(out, fmt.Sprintf("ERR apply return type %v", t))
							}
						}
					}
				} else {
					for i := 0; i < l; i++ {
						out = redcon.AppendOK(out)
					}
				}

				delete(commitMap, shardID)
			}
		}

		for action == evio.None {
			//if cmd != nil {
			//	commands = append(commands, cmd)
			//}
			// Read next command.
			packet, complete, args, _, data, err = redcon.ReadNextCommand2(data, args[:0])

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
				// Parse Command
				cmd = s.handler.Parse(c, packet, args)
				cmdCount++

				if cmd == nil {
					cmd = ERR(fmt.Sprintf("ERR command '%s' not found", strings.ToUpper(string(args[0]))))
				}

				//commands = append(commands, cmd)

				if cmd.IsWrite() {
					key := string(args[1])
					shardID := s.handler.ShardID(key)

					b := make([]byte, len(packet))
					copy(b, packet)

					// Add to commit log.
					if commitMap == nil {
						commitMap = make(map[int]*struct {
							cmds []Command
							buf  []byte
						})
						commitMap[shardID] = &struct {
							cmds []Command
							buf  []byte
						}{
							cmds: []Command{cmd},
							buf:  b,
						}
					} else {
						group, ok := commitMap[shardID]
						if !ok {
							commitMap[shardID] = &struct {
								cmds []Command
								buf  []byte
							}{
								cmds: []Command{cmd},
								buf:  b,
							}
						} else {
							group.cmds = append(group.cmds, cmd)
							group.buf = append(group.buf, b...)
						}
					}

					// Commit
					//future, err := s.handler.Commit(c, shardID, packet)
					//
					//if err != nil {
					//	out = redcon.AppendError(out, "ERR "+err.Error())
					//} else if err = future.Error(); err != nil {
					//	out = redcon.AppendError(out, "ERR "+err.Error())
					//} else if resp := future.Response(); resp != nil {
					//	if buf, ok := resp.([]byte); ok {
					//		out = append(out, buf...)
					//	} else {
					//		switch t := resp.(type) {
					//		case string:
					//			out = redcon.AppendBulkString(out, t)
					//		default:
					//			out = redcon.AppendError(out, fmt.Sprintf("ERR apply return type %v", t))
					//		}
					//	}
					//} else {
					//	out = redcon.AppendOK(out)
					//}

					//_ = future
					//_ = err
					//
					//out = redcon.AppendOK(out)
				} else {
					// Commit if necessary
					if len(commitMap) > 0 {
						flush()
					}

					// Append directly.
					out = cmd.Invoke(out)
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
			}
		}

		// Copy partial Cmd data if any.
		c.end(data)

		if action == evio.Close {
			return
		}

		if cmd == nil {
			return out, c.evaction
		}

		// Flush commit buffer
		if len(commitMap) > 0 {
			flush()
		}

		//e.Logger.Debug().Msgf("%d commands", cmdCount)

		return out, c.evaction
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
