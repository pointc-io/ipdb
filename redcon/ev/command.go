package evred

import (
	"github.com/pointc-io/ipdb/redcon"
)

var (
	ok = redcon.AppendOK(nil)
)

//
//
//
type Command interface {
	IsWrite() bool

	IsAsync() bool

	// Invoke happens on the EvLoop
	Invoke(ctx *CommandContext) []byte
}

//
//
//
type Cmd struct {
	Command
}

func (c *Cmd) IsWrite() bool {
	return false
}

func (c *Cmd) IsAsync() bool {
	return false
}

func (c *Cmd) Invoke(ctx *CommandContext) []byte {
	return ctx.AppendError("ERR not implemented")
}

//
//
//
func RAW(b []byte) Command {
	return RawCmd(b)
}

type RawCmd []byte


func (c RawCmd) IsWrite() bool {
	return false
}

func (c RawCmd) IsAsync() bool {
	return false
}

func (c RawCmd) Invoke(ctx *CommandContext) []byte {
	return append(ctx.Out, c...)
}

//
func OK() Command {
	return RAW(ok)
}

func Int(value int) Command {
	return RAW(redcon.AppendInt(nil, int64(value)))
}

func Bulk(b []byte) Command {
	return RAW(redcon.AppendBulk(nil, b))
}

func BulkString(str string) Command {
	return RAW(redcon.AppendBulkString(nil, str))
}

//
//
//
func ERR(message string) Command {
	return RAW(redcon.AppendError(nil, message))
}

//
//
//
func ERROR(err error) Command {
	return RAW(redcon.AppendError(nil, err.Error()))
}

// ERR command
type ErrCmd struct {
	Command
	Result []byte
}

func (c *ErrCmd) Invoke(out []byte) []byte {
	return append(out, c.Result...)
}

var WRITE = &WriteCmd{}

//
//
//
type WriteCmd struct {
	Command
}

func (c *WriteCmd) IsWrite() bool {
	return true
}

func (c *WriteCmd) IsAsync() bool {
	return false
}

//
// Command needs to be dispatched and ran on a Worker
//
type BgCmd struct {
	Command
}

func (c *BgCmd) Invoke(ctx *CommandContext) []byte {
	return ctx.Out
}
