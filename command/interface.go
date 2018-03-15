package command

import (
	"github.com/pointc-io/sliced/redcon"
	"github.com/pointc-io/sliced/item"
)

type Durability int

const (
	Low    Durability = -1
	Medium Durability = 0
	High   Durability = 1
)

type Handler interface {
	ShardID(key string) int

	Parse(ctx *Context) Command

	Commit(ctx *Context)
}

// Represents a series of pipelined Commands
type Context struct {
	Conn    Conn     // Connection
	Out     []byte   // Output buffer to write to connection
	Index   int      // Index of command
	ShardID int      // Shard ID if calculated
	Name    string   // Current command name
	Key     string   // Key if it exists
	Packet  []byte   // Raw byte slice of current command
	Args    [][]byte // Current command args

	// Transactional holders
	Set *item.Set

	Changes map[int]*ChangeSet
}

type ChangeSet struct {
	Cmds []Command
	Data []byte
}

func (c *Context) Int(index int, or int) int {
	return or
}

func (c *Context) AppendOK() []byte {
	return redcon.AppendOK(c.Out)
}

func (c *Context) AppendNull() []byte {
	return redcon.AppendNull(c.Out)
}

func (c *Context) AppendError(msg string) []byte {
	return redcon.AppendError(c.Out, msg)
}

func (c *Context) AppendBulkString(bulk string) []byte {
	return redcon.AppendBulkString(c.Out, bulk)
}

func (c *Context) ApppendBulk(bulk []byte) []byte {
	return redcon.AppendBulk(c.Out, bulk)
}

func (c *Context) ApppendInt(val int) []byte {
	return redcon.AppendInt(c.Out, int64(val))
}

func (c *Context) ApppendInt64(val int64) []byte {
	return redcon.AppendInt(c.Out, val)
}

func (c *Context) Commit() {
	if len(c.Changes) > 0 {
		c.Conn.Handler().Commit(c)
	}
}

func (c *Context) HasChanges() bool {
	return len(c.Changes) > 0
}

func (c *Context) AddChange(cmd Command, data []byte) {
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
