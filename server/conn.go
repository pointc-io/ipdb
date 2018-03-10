package server
//
//import (
//	"sync"
//
//	"github.com/pointc-io/ipdb/evio"
//	"github.com/pointc-io/ipdb/worker"
//	"github.com/pointc-io/ipdb/action"
//)
//
//var Workers = worker.Workers
//var MaxConnBacklog = 2048
//
//type dbConn struct {
//	id       int
//	addr     string
//	ev       *EventLoop
//	evaction evio.Action
//	done     bool
//
//	mu  sync.Mutex
//	in  []byte
//	out []byte // Out buffer
//	//outb []byte
//
//	backlog        []action.Command
//	backlogOverage int
//	dispatched     action.Command
//}
//
//// Incoming REDIS protocol command
//func (c *dbConn) incoming(b []byte, name string, args [][]byte) (out []byte) {
//	out = b
//	// Parse Command
//	cmd := c.parseCommand(name, args)
//
//	if cmd == nil {
//		cmd = action.ERR(0, "Command '"+name+"' not found")
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
//
//func (c *dbConn) dispatch(cmd action.Command) {
//	c.dispatched = cmd
//	Workers.Dispatch(c)
//}
//
//func (c *dbConn) Run() {
//	// Was the job already canceled?
//	c.mu.Lock()
//	if c.dispatched == nil {
//		c.mu.Unlock()
//		return
//	}
//	c.mu.Unlock()
//
//	l := len(c.out)
//	// Run job.
//	c.out = c.dispatched.Background(c.out)
//
//	if len(c.out) == l {
//		c.out = action.ERR(0, "Command not implemented").Invoke(c.out)
//	}
//
//	c.mu.Lock()
//	c.dispatched = nil
//	c.mu.Unlock()
//
//	// Notify event loop of our write.
//	c.wake()
//}
//
//// Inform the event loop to close this connection.
//func (c *dbConn) close() {
//	c.evaction = evio.Close
//	c.wake()
//}
//
//// Called when the event loop has closed this connection.
//func (c *dbConn) closed() {
//	c.done = true
//}
//
//// Asks the event loop to schedule a write event for this connection.
//func (c *dbConn) wake() {
//	c.ev.Wake(c.id)
//}
//
//// Invoked on the event loop thread.
//func (c *dbConn) woke() (out []byte, action evio.Action) {
//	// Set output buffer
//	c.mu.Lock()
//	if c.dispatched != nil {
//		c.mu.Unlock()
//		return
//	}
//	if len(out) == 0 {
//		out = c.out
//	} else {
//		out = append(out, c.out...)
//	}
//	// Clear connection's out buffer
//	c.out = nil
//	c.mu.Unlock()
//
//	// Empty backlog.
//	for i, cmd := range c.backlog {
//		before := len(out)
//		out = cmd.Invoke(out)
//
//		// Does it need to be dispatched?
//		if len(out) == before {
//			// Update backlog.
//			if i == len(c.backlog)-1 {
//				c.backlog = nil
//			} else {
//				c.backlog = c.backlog[i+1:]
//			}
//			// Command needs to be dispatched.
//			c.dispatched = cmd
//			Workers.Dispatch(c)
//			return
//		} else {
//			// Update backlog.
//			if i == len(c.backlog)-1 {
//				c.backlog = nil
//			} else {
//				c.backlog = c.backlog[i+1:]
//			}
//			return
//		}
//	}
//
//	// Backlog has been fully drained.
//	c.backlog = nil
//	return
//}
//
//// Begin accepts a new packet and returns a working sequence of
//// unprocessed bytes.
//func (c *dbConn) begin(packet []byte) (data []byte) {
//	data = packet
//	if len(c.in) > 0 {
//		c.in = append(c.in, data...)
//		data = c.in
//	}
//	return data
//}
//
//// End shift the stream to match the unprocessed data.
//func (c *dbConn) end(data []byte) {
//	if len(data) > 0 {
//		if len(data) != len(c.in) {
//			c.in = append(c.in[:0], data...)
//		}
//	} else if len(c.in) > 0 {
//		c.in = c.in[:0]
//	}
//
//	//if len(c.out) > 0 {
//	//	c.outb = c.out
//	//	c.out = nil
//	//}
//}
