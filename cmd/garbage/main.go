// Copyright 2017 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package garbage

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
	"sync"
	"context"
	"sync/atomic"
	"runtime"

	"github.com/pointc-io/evio"
	"github.com/tidwall/redcon"

	"github.com/pointc-io/pool/pbufio"
	"github.com/pointc-io/pool/pbytes"
	"github.com/rcrowley/go-metrics"
	"net"
)

var Workers *WorkerPool
var ReaderPool = pbufio.DefaultReaderPool
var WriterPool = pbufio.DefaultWriterPool
var BufferPool = pbytes.DefaultPool

var ctx context.Context

func main() {
	var port int
	var unixsocket string
	var stdlib bool
	flag.IntVar(&port, "port", 6380, "server port")
	flag.StringVar(&unixsocket, "unixsocket", "socket", "unix socket")
	flag.BoolVar(&stdlib, "stdlib", false, "use stdlib")
	flag.Parse()

	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}

	Workers = NewWorkerPool(context.Background(), 0, 1000)

	//go main1("1", listener, port, unixsocket, stdlib)
	//go main1("3", listener, port, unixsocket, stdlib)
	main1("2", listener, port, unixsocket, stdlib)
}

func main1(thread string, listener net.Listener, port int, unixsocket string, stdlib bool) {
	//runtime.LockOSThread()

	//ctx = context.Background()

	var srv evio.Server
	var conns = make(map[int]*conn)
	var keys = make(map[string]string)
	var events evio.Events
	events.Serving = func(srvin evio.Server) (action evio.Action) {
		srv = srvin
		log.Printf("redis server started on port %d", port)
		if unixsocket != "" {
			log.Printf("redis server started at %s", unixsocket)
		}
		if stdlib {
			log.Printf("stdlib")
		}
		return
	}
	wgetids := make(map[int]time.Time)
	events.Opened = func(id int, info evio.Info) (out []byte, opts evio.Options, action evio.Action) {
		//fmt.Println("Conn opened on thread " + thread)
		c := &conn{fd: id}
		//if !wgetids[id].IsZero() {
		//	delete(wgetids, id)
		//	c.wget = true
		//}
		conns[id] = c
		//if c.wget {
		//	log.Printf("opened: %d, wget: %t, laddr: %v, laddr: %v", id, c.wget, info.LocalAddr, info.RemoteAddr)
		//}
		//if c.wget {
		//	out = []byte("GET / HTTP/1.0\r\n\r\n")
		//}
		return
	}
	events.Tick = func() (delay time.Duration, action evio.Action) {
		now := time.Now()
		for id, t := range wgetids {
			if now.Sub(t) > time.Second {
				srv.Wake(id)
			}
		}
		delay = time.Second
		return
	}
	events.Closed = func(id int, err error) (action evio.Action) {
		c := conns[id]
		_ = c
		//if c.wget {
		//	fmt.Printf("closed %d %v\n", id, err)
		//}
		delete(conns, id)
		return
	}
	events.Postwrite = func(id int, amount, remaining int) (action evio.Action) {
		c, ok := conns[id]
		if !ok {
			return
		}
		_ = c

		return
	}
	events.Data = func(id int, in []byte) (out []byte, action evio.Action) {
		c := conns[id]
		//if c.wget {
		//	print(string(in))
		//	return
		//}
		if in == nil {
			out = redcon.AppendOK(out)
			return
		}
		data := c.is.Begin(in)
		var n int
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
			if !complete {
				break
			}
			if len(args) > 0 {
				n++
				switch strings.ToUpper(string(args[0])) {
				default:
					out = redcon.AppendError(out, "ERR unknown command '"+string(args[0])+"'")
				case "SLEEP":
					if len(args) != 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						seconds, _ := strconv.Atoi(string(args[1]))
						//seconds, _ := strconv.ParseInt(string(args[1]), 10, 63)
						Schedule(c, func(c *conn) {
							time.Sleep(time.Second * time.Duration(seconds))
							srv.Wake(id)
						})
					}
				case "WGET":
					if len(args) != 3 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						n, _ := strconv.ParseInt(string(args[2]), 10, 63)
						cid := srv.Dial("tcp://"+string(args[1]), time.Duration(n)*time.Second)
						if cid == 0 {
							out = redcon.AppendError(out, "failed to dial")
						} else {
							wgetids[cid] = time.Now()
							out = redcon.AppendOK(out)
						}
					}
				case "PING":
					if len(args) > 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else if len(args) == 2 {
						out = redcon.AppendBulk(out, args[1])
					} else {
						out = redcon.AppendString(out, "PONG")
					}
				case "ECHO":
					if len(args) != 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						out = redcon.AppendBulk(out, args[1])
					}
				case "SHUTDOWN":
					out = redcon.AppendString(out, "OK")
					action = evio.Shutdown

				case "QUIT":
					out = redcon.AppendString(out, "OK")
					action = evio.Close
				case "GET":
					if len(args) != 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						val, ok := keys[string(args[1])]
						if !ok {
							out = redcon.AppendNull(out)
						} else {
							out = redcon.AppendBulkString(out, val)
						}
					}
				case "SET":
					if len(args) != 3 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						keys[string(args[1])] = string(args[2])
						out = redcon.AppendString(out, "OK")
					}
				case "DEL":
					if len(args) < 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						var n int
						for i := 1; i < len(args); i++ {
							if _, ok := keys[string(args[1])]; ok {
								n++
								delete(keys, string(args[1]))
							}
						}
						out = redcon.AppendInt(out, int64(n))
					}
				case "FLUSHDB":
					keys = make(map[string]string)
					out = redcon.AppendString(out, "OK")
				}
			}
		}
		c.is.End(data)
		return
	}
	var ssuf string
	if stdlib {
		ssuf = "-net"
	}
	addrs := []string{fmt.Sprintf("tcp"+ssuf+"://:%d", port)}
	if unixsocket != "" {
		addrs = append(addrs, fmt.Sprintf("unix"+ssuf+"://%s", unixsocket))
	}

	err := evio.ServeListener(events, stdlib, listener)
	if err != nil {
		log.Fatal(err)
	}
}

type conn struct {
	fd   int
	is   evio.InputStream
	addr string

	mu      sync.Mutex
	b       []byte // Out buffer
	outlog  [][]byte
	backlog []*Message
	worker  *Worker // Worker
}

type Message struct {
	complete bool
	args     [][]byte
	kind     redcon.Kind
	leftover []byte
	err      error
}

func (c *Message) handle() (out []byte, action evio.Action) {
	return
}

func (c *conn) onClose() {
	c.mu.Lock()
	if c.worker != nil {
		c.worker.Release()
	}
	if c.b != nil {
		BufferPool.Put(c.b)
	}
	c.mu.Unlock()
}

func (c *conn) onData(in []byte) (out []byte, action evio.Action) {
	if in == nil {
		return c.onWoke(in)
	}
	return
}

//
func (c *conn) onWoke(in []byte) (out []byte, action evio.Action) {
	if c.worker != nil {
		c.worker.Release()
		c.worker = nil
	}
	return
}

func Schedule(c *conn, run func(c *conn)) bool {
	if c.worker == nil {
		c.worker = Workers.Get()
	}
	if c.worker == nil {
		// Too busy
		return false
	}
	return true
}

var controller *Controller

type Controller struct {
	// static values
	host      string
	port      int
	http      bool
	dir       string
	started   time.Time
	maxMemory uint64

	statsTotalConns        aint // counter for total connections
	statsTotalCommands     aint // counter for total commands
	stopBackgroundExpiring abool
	stopWatchingMemory     abool
	stopWatchingAutoGC     abool
	outOfMemory            abool

	srv   evio.Server
	conns map[int]*conn
	keys  map[string]string

	mu sync.RWMutex
}

func ListenAndServe(host string, port int) error {
	controller = &Controller{
		host: host,
		port: port,

		conns: make(map[int]*conn),
		keys:  make(map[string]string),
	}

	//srv := c.srv
	conns := controller.conns
	keys := controller.keys
	var events evio.Events
	events.Serving = func(srvin evio.Server) (action evio.Action) {
		controller.srv = srvin
		log.Printf("redis server started on port %d", port)
		return
	}
	events.Opened = func(id int, info evio.Info) (out []byte, opts evio.Options, action evio.Action) {
		c := &conn{fd: id}
		//if !wgetids[id].IsZero() {
		//	delete(wgetids, id)
		//	c.wget = true
		//}
		conns[id] = c
		//if c.wget {
		//	log.Printf("opened: %d, wget: %t, laddr: %v, laddr: %v", id, c.wget, info.LocalAddr, info.RemoteAddr)
		//}
		//if c.wget {
		//	out = []byte("GET / HTTP/1.0\r\n\r\n")
		//}
		return
	}
	events.Tick = func() (delay time.Duration, action evio.Action) {
		//now := time.Now()
		//for id, t := range wgetids {
		//	if now.Sub(t) > time.Second {
		//		controller.srv.Wake(id)
		//	}
		//}
		delay = time.Second
		return
	}
	events.Closed = func(id int, err error) (action evio.Action) {
		c := conns[id]
		_ = c
		delete(conns, id)
		return
	}
	events.Data = func(id int, in []byte) (out []byte, action evio.Action) {
		c, ok := conns[id]
		if !ok {
			action = evio.Close
			return
		}

		if in == nil {
			return c.onWoke(in)
			//out = redcon.AppendOK(out)
			//return
		}
		data := c.is.Begin(in)
		//var n int64
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
				break
			}
			if len(args) > 0 {
				//n++
				switch strings.ToUpper(string(args[0])) {
				default:
					out = redcon.AppendError(out, "ERR unknown command '"+string(args[0])+"'")

				case "SLEEP":
					if len(args) != 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						seconds, _ := strconv.Atoi(string(args[1]))
						//seconds, _ := strconv.ParseInt(string(args[1]), 10, 63)
						if !Schedule(c, func(c *conn) {
							time.Sleep(time.Second * time.Duration(seconds))
							controller.srv.Wake(id)
						}) {
							out = redcon.AppendError(out, "ERR too busy")
						}
					}

				case "PING":
					if len(args) > 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else if len(args) == 2 {
						out = redcon.AppendBulk(out, args[1])
					} else {
						out = redcon.AppendString(out, "PONG")
					}

				case "ECHO":
					if len(args) != 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						out = redcon.AppendBulk(out, args[1])
					}

				case "SHUTDOWN":
					out = redcon.AppendString(out, "OK")
					action = evio.Shutdown

				case "QUIT":
					out = redcon.AppendString(out, "OK")
					action = evio.Close

				case "GET":
					if len(args) != 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						val, ok := keys[string(args[1])]
						if !ok {
							out = redcon.AppendNull(out)
						} else {
							out = redcon.AppendBulkString(out, val)
						}
					}

				case "SET":
					if len(args) != 3 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						keys[string(args[1])] = string(args[2])
						out = redcon.AppendString(out, "OK")
					}

				case "DEL":
					if len(args) < 2 {
						out = redcon.AppendError(out, "ERR wrong number of arguments for '"+string(args[0])+"' command")
					} else {
						var n int
						for i := 1; i < len(args); i++ {
							if _, ok := keys[string(args[1])]; ok {
								n++
								delete(keys, string(args[1]))
							}
						}
						out = redcon.AppendInt(out, int64(n))
					}

				case "FLUSHDB":
					keys = make(map[string]string)
					out = redcon.AppendString(out, "OK")
				}
			}
		}
		c.is.End(data)
		return
	}

	go controller.watchOutOfMemory()

	var ssuf string
	//if stdlib {
	//	ssuf = "-net"
	//}
	addrs := []string{fmt.Sprintf("tcp"+ssuf+"://:%d", port)}
	//if unixsocket != "" {
	//	addrs = append(addrs, fmt.Sprintf("unix"+ssuf+"://%s", unixsocket))
	//}
	err := evio.Serve(events, addrs...)
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (c *Controller) watchOutOfMemory() {
	t := time.NewTicker(time.Second * 2)
	defer t.Stop()
	var mem runtime.MemStats
	for range t.C {
		func() {
			if c.stopWatchingMemory.on() {
				return
			}
			oom := c.outOfMemory.on()
			//if c.config.maxMemory() == 0 {
			//	if oom {
			//		c.outOfMemory.set(false)
			//	}
			//	return
			//}
			if oom {
				runtime.GC()
			}
			runtime.ReadMemStats(&mem)
			c.outOfMemory.set(mem.HeapAlloc > c.maxMemory)
		}()
	}
}

type Request interface {
}

type requestQueue struct {
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

const (
	DefaultMax = 1024
)

type WorkerPool struct {
	name   string
	ctx    context.Context
	cancel context.CancelFunc
	pool   *sync.Pool
	mu     *sync.Mutex
	min    int64
	max    int64

	workers []*Worker

	// Track Worker supply
	supply    metrics.Counter
	maxSupply metrics.Counter
	forged    metrics.Counter
	exhausted metrics.Counter
	gets      metrics.Counter
	puts      metrics.Counter

	wg *sync.WaitGroup
}

func (w *WorkerPool) Name() string {
	return w.name
}

func NewWorkerPool(ctx context.Context, min, max int) *WorkerPool {
	mu := &sync.Mutex{}
	if min < 0 {
		min = 0
	}
	if max < min {
		max = min
	}

	c, cancel := context.WithCancel(ctx)

	w := &WorkerPool{
		ctx:     c,
		cancel:  cancel,
		mu:      mu,
		min:     int64(min),
		max:     int64(max),
		workers: make([]*Worker, min),

		wg:        &sync.WaitGroup{},
		supply:    metrics.NewCounter(),
		maxSupply: metrics.NewCounter(),
		forged:    metrics.NewCounter(),
		exhausted: metrics.NewCounter(),
		gets:      metrics.NewCounter(),
		puts:      metrics.NewCounter(),
	}

	w.pool = new(sync.Pool)

	// Spin up to satisfy min.
	for i := 0; i < min; i++ {
		w.Get().Release()
	}

	return w
}

func (w *WorkerPool) Register(registry metrics.Registry) {
	name := "WorkerPool." + w.name
	registry.Register(name+".supply", w.supply)
	registry.Register(name+".maxSupply", w.maxSupply)
	registry.Register(name+".forged", w.forged)
	registry.Register(name+".exhausted", w.exhausted)
	registry.Register(name+".gets", w.gets)
	registry.Register(name+".puts", w.puts)
}

func (w *WorkerPool) Stop() {
	w.mu.Lock()
	w.cancel()
	w.mu.Unlock()
	w.wg.Wait()
}

func (w *WorkerPool) Get() *Worker {
	w.gets.Inc(1)

	worker := w.pool.Get()
	if worker != nil {
		w.gets.Inc(1)
		return worker.(*Worker)
	}

	// Barrier
	w.mu.Lock()
	defer w.mu.Unlock()

	// Increase the supply.
	w.supply.Inc(1)
	// Did we exceed the max?
	if w.max > 0 && w.supply.Count() >= w.max {
		// Exhausted.
		w.exhausted.Inc(1)
		// Decrease supply.
		w.supply.Dec(1)
		// Return nil. Out of Memory
		return nil
	}

	// Increase forged count
	w.forged.Inc(1)

	// Create new Worker.
	wkr := &Worker{
		pool: w,
		quit: make(chan struct{}),
		jobs: make(chan Job, 1),
	}

	// Add to tracking slice
	w.workers = append(w.workers, wkr)

	// Track goroutine exit
	w.wg.Add(1)
	// Start worker
	go wkr.run(w.ctx, w.wg)

	// Return Worker
	return wkr
}

func (w *WorkerPool) GetTimeout(duration time.Duration) *Worker {
	worker := w.pool.Get()
	if worker == nil {
		return nil
	}
	return worker.(*Worker)
}

func (w *WorkerPool) Put(worker *Worker) {
	w.puts.Inc(1)
	w.pool.Put(worker)
}

// Maintains a cached goroutine that can process a job at a time
type Worker struct {
	pool *WorkerPool
	quit chan struct{}
	jobs chan Job
}

func (w *Worker) Release() {
	w.pool.Put(w)
}

func (w *Worker) stop() {
	close(w.quit)
	close(w.jobs)
}

type Job func() interface{}

func (w *Worker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-w.quit:
			w.stop()
			return
		case <-ctx.Done():
			w.stop()
			return
		case job, ok := <-w.jobs:
			if !ok {
				w.stop()
				return
			}
			job()
		}
	}

	w.stop()
}

// InputStream is a helper type for managing input streams inside the
// Data event.
type InputStream struct{ b []byte }

// Begin accepts a new packet and returns a working sequence of
// unprocessed bytes.
func (is *InputStream) Begin(packet []byte) (data []byte) {
	data = packet
	if len(is.b) > 0 {
		is.b = append(is.b, data...)
		data = is.b
	}
	return data
}

// End shift the stream to match the unprocessed data.
func (is *InputStream) End(data []byte) {
	if len(data) > 0 {
		if len(data) != len(is.b) {
			is.b = append(is.b[:0], data...)
		}
	} else if len(is.b) > 0 {
		is.b = is.b[:0]
	}
}
