package evred

import (
	"sync"
	"github.com/pointc-io/ipdb/evio"
)

var maxCommandBacklog = 2048

type Conn struct {
	id       int
	addr     string
	ev       *EvLoop
	evaction evio.Action
	done     bool

	mu    sync.Mutex
	in    []byte
	out   []byte // Out buffer
	multi bool
	flush bool

	backlog        []Command
	backlogOverage int
	dispatched     Command

	multis []Command

	handler Handler

	statsTotalCommands   uint64
	statsTotalUpstream   uint64
	statsTotalDownstream uint64

	Consistency ApplyMode

	ctx *CommandContext
}

// Incoming REDIS protocol Cmd
//func (c *Conn) incoming(b []byte, name string, args [][]byte) (out []byte) {
//	out = b
//
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

func (c *Conn) Close() error {
	c.evaction = evio.Close
	return nil
}

func (c *Conn) SetHandler(handler Handler) Handler {
	prev := c.handler
	c.handler = handler
	return prev
}

func (c *Conn) Multi() {
	if c.multi {
		c.multi = false
		c.flush = true
	} else {
		c.multi = true
		c.flush = false
	}
}

func (c *Conn) dispatch(cmd Command) {
	c.dispatched = cmd
	Workers.Dispatch(c)
}

func (c *Conn) Run() {
	//// Was the job already canceled?
	//c.mu.Lock()
	//if c.dispatched == nil {
	//	c.mu.Unlock()
	//	return
	//}
	//c.mu.Unlock()
	//
	//l := len(c.out)
	//// Run job.
	//c.out = c.dispatched.Invoke(c.ctx)
	//
	//if len(c.out) == l {
	//	c.out = ERR("Command not implemented").Invoke(c.out)
	//}
	//
	//c.mu.Lock()
	//c.dispatched = nil
	//c.mu.Unlock()
	//
	//// Notify event loop of our write.
	//c.wake()
}

// Inform the event loop to close this connection.
func (c *Conn) close() {
	c.evaction = evio.Close
	c.wake()
}

// Called when the event loop has closed this connection.
func (c *Conn) closed() {
	c.done = true
}

// Asks the event loop to schedule a write event for this connection.
func (c *Conn) wake() {
	c.ev.Wake(c.id)
}

// Invoked on the event loop thread.
func (c *Conn) woke() (out []byte, action evio.Action) {
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
		out = cmd.Invoke(nil)

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
func (c *Conn) begin(packet []byte) (data []byte) {
	data = packet
	if len(c.in) > 0 {
		c.in = append(c.in, data...)
		data = c.in
	}
	return data
}

// End shift the stream to match the unprocessed data.
func (c *Conn) end(data []byte) {
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
