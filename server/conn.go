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
	evloop      *evio.Server
	evaction evio.Action
	done bool

	mu      sync.Mutex
	b       []byte // Out buffer
	outlog  [][]byte
	backlog []action.Command
	current action.Command
}

// Incoming REDIS protocol command
func (c *dbConn) incoming(name string, args [][]byte) (out []byte) {
	// Parse Command
	cmd := c.parseCommand(name, args)

	if cmd == nil {
		cmd = action.ERR(0, "Command '"+name+"' not found")
	}

	if c.current == nil {
		b := cmd.Invoke()
		if len(b) > 0 {
			out = append(out, b...)
		} else {
			c.current = cmd
			Workers.Dispatch(cmd)
		}
	} else {
		// Append onto backlog
		c.backlog = append(c.backlog, cmd)
	}

	return
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
	out = c.b
	c.mu.Unlock()

	// Empty backlog.
loop:
	for _, cmd := range c.backlog {
		r := cmd.Invoke()
		if len(r) == 0 {
			break loop
		}
		out = append(out, r...)
	}

	// Clear connection buffer.
	c.b = nil
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
