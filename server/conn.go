package server

import (
	"sync"

	"github.com/pointc-io/ipdb/evio"
	"github.com/pointc-io/ipdb/worker"
	"github.com/pointc-io/ipdb/action"
)

var Workers = worker.DefaultPool

type dbConn struct {
	id       int
	is       InputStream
	addr     string
	evloop   *evio.Server
	evaction evio.Action
	done     bool

	mu         sync.Mutex
	out        []byte // Out buffer
	backlog    []action.Command
	dispatched action.Command
}

// Incoming REDIS protocol command
func (c *dbConn) incoming(name string, args [][]byte) (out []byte) {
	// Parse Command
	cmd := c.parseCommand(name, args)

	if cmd == nil {
		cmd = action.ERR(0, "Command '"+name+"' not found")
	}

	if c.dispatched == nil {
		before := len(out)
		out = cmd.Invoke(out)

		if len(out) == before {
			c.dispatch(cmd)
		}
	} else {
		// Append onto backlog
		c.backlog = append(c.backlog, cmd)
	}

	return
}

func (c *dbConn) dispatch(cmd action.Command) {
	c.dispatched = cmd
	Workers.Dispatch(c)
}

func (c *dbConn) Run() {
	c.mu.Lock()
	if c.dispatched == nil {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	l := len(c.out)
	// Run job.
	c.out = c.dispatched.Background(c.out)

	if len(c.out) == l {
		c.out = action.ERR(0, "Command not implemented").Invoke(c.out)
	}

	c.mu.Lock()
	c.dispatched = nil
	c.mu.Unlock()

	c.wake()
}

func (c *dbConn) close() {
	c.evaction = evio.Close
	c.wake()
}

func (c *dbConn) closed() {
	c.done = true
}

func (c *dbConn) wake() {
	c.evloop.Wake(c.id)
}

//
func (c *dbConn) woke() (out []byte, action evio.Action) {
	// Set output buffer
	c.mu.Lock()
	if c.dispatched != nil {
		c.mu.Unlock()
		return
	}
	if len(out) == 0 {
		out = c.out
	} else {
		out = append(out, c.out...)
	}
	// Clear connection's out buffer
	c.out = nil
	c.mu.Unlock()

	// Empty backlog.
loop:
	for i, cmd := range c.backlog {
		before := len(out)
		out = cmd.Invoke(out)
		if len(out) == before {
			c.backlog = c.backlog[i+1:]
			break loop
		} else {
			c.dispatched = cmd
			Workers.Dispatch(c)
		}
	}
	return
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
