package evred

import (
	"sync"
	"net"
	"github.com/pointc-io/ipdb/evio"
	"github.com/pointc-io/ipdb/redcon"
)

var Workers = redcon.DefaultPool
var MaxConnBacklog = 2048

type RedConn struct {
	id       int
	addr     string
	ev       *EvLoop
	evaction evio.Action
	done     bool

	mu  sync.Mutex
	in  []byte
	out []byte // Out buffer
	//outb []byte

	backlog        []Command
	backlogOverage int
	dispatched     Command

	detached   net.Conn
	detachedFn func(conn net.Conn)
}

// Incoming REDIS protocol Cmd
//func (c *RedConn) incoming(b []byte, name string, args [][]byte) (out []byte) {
//	out = b
//	c.handler(c, args)
//	// Parse Command
//	cmd := c.handler.parseCommand(name, args)
//
//	if cmd == nil {
//		cmd = ERR(0, "Command '"+name+"' not found")
//	}
//
//	if c.dispatched == nil {
//		before := len(out)
//		out = cmd.Invoke(out)
//
//		if len(out) == before {
//			c.dispatch(cmd)
//		}
//	} else {
//		// Append onto backlog
//		c.backlog = append(c.backlog, cmd)
//	}
//
//	return
//}

func (c *RedConn) dispatch(cmd Command) {
	c.dispatched = cmd
	Workers.Dispatch(c)
}

func (c *RedConn) Run() {
	// Was the job already canceled?
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
		c.out = ERR(0, "Command not implemented").Invoke(c.out)
	}

	c.mu.Lock()
	c.dispatched = nil
	c.mu.Unlock()

	// Notify event loop of our write.
	c.wake()
}

// Inform the event loop to close this connection.
func (c *RedConn) close() {
	c.evaction = evio.Close
	c.wake()
}

// Called when the event loop has closed this connection.
func (c *RedConn) closed() {
	c.done = true
}

// Asks the event loop to schedule a write event for this connection.
func (c *RedConn) wake() {
	c.ev.Wake(c.id)
}

// Invoked on the event loop thread.
func (c *RedConn) woke() (out []byte, action evio.Action) {
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
	for i, cmd := range c.backlog {
		before := len(out)
		out = cmd.Invoke(out)

		// Does it need to be dispatched?
		if len(out) == before {
			// Update backlog.
			if i == len(c.backlog)-1 {
				c.backlog = nil
			} else {
				c.backlog = c.backlog[i+1:]
			}
			// Command needs to be dispatched.
			c.dispatched = cmd
			Workers.Dispatch(c)
			return
		} else {
			// Update backlog.
			if i == len(c.backlog)-1 {
				c.backlog = nil
			} else {
				c.backlog = c.backlog[i+1:]
			}
			return
		}
	}

	// Backlog has been fully drained.
	c.backlog = nil
	return
}

// Begin accepts a new packet and returns a working sequence of
// unprocessed bytes.
func (c *RedConn) begin(packet []byte) (data []byte) {
	data = packet
	if len(c.in) > 0 {
		c.in = append(c.in, data...)
		data = c.in
	}
	return data
}

// End shift the stream to match the unprocessed data.
func (c *RedConn) end(data []byte) {
	if len(data) > 0 {
		if len(data) != len(c.in) {
			c.in = append(c.in[:0], data...)
		}
	} else if len(c.in) > 0 {
		c.in = c.in[:0]
	}

	//if len(c.out) > 0 {
	//	c.outb = c.out
	//	c.out = nil
	//}
}

func (c *RedConn) onDetached(conn net.Conn) {
	c.mu.Lock()
	c.detached = conn
	fn := c.detachedFn
	c.mu.Unlock()
	if fn != nil {
		fn(conn)
	}
}

func (c *RedConn) Detach(fn func(conn net.Conn)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.detached == nil {
		//c.detached = c.ev.Ev.
		c.evaction = evio.Detach
		c.detachedFn = fn
	}
}
