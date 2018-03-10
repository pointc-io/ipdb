package evred

import (
	"github.com/pointc-io/ipdb/redcon"
)

//
//
//
type Command interface {
	IsWrite() bool

	Commit(out []byte, packet []byte) []byte

	// Invoke happens on the EvLoop
	Invoke(out []byte) []byte

	Background(out []byte) []byte
}

//
//
//
type Cmd struct {
	Command
}

func (c *Cmd) Commit(out []byte, packet []byte) []byte {
	out = append(out, packet...)
	return out
}

func (c *Cmd) IsWrite() bool {
	return false
}

func (c *Cmd) Invoke(out []byte) []byte {
	return redcon.AppendError(out, "ERR not implemented")
}

func (c *Cmd) Background(out []byte) []byte {
	return redcon.AppendError(out, "ERR not implemented")
}

//
//
//
func RAW(b []byte) Command {
	return RawCmd(b)
}

type RawCmd []byte

func (c RawCmd) Commit(out []byte, packet []byte) []byte {
	out = append(out, packet...)
	return out
}

func (c RawCmd) IsWrite() bool {
	return false
}

func (c RawCmd) Background(out []byte) []byte {
	return append(out, c...)
}

func (c RawCmd) Invoke(out []byte) []byte {
	return append(out, c...)
}

//
//
//
func ERR(message string) *ErrCmd {
	return &ErrCmd{
		Result: redcon.AppendError(nil, message),
	}
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

//
// Command needs to be dispatched and ran on a Worker
//
type BgCmd struct {
	Command
}

func (c *BgCmd) Invoke(out []byte) []byte {
	return out
}
