package action

import (
	"github.com/pointc-io/ipdb/redcon"
)

var NeedsDispatch = []byte{}

type Command interface {
	// Invoke happens on the EventLoop
	Invoke(out []byte) []byte

	Background(out []byte) []byte
}

type command struct {
	Command
}

func (c *command) Invoke(out []byte) []byte {
	return redcon.AppendError(out, "ERR not implemented")
}

func (c *command) Background(out []byte) []byte {
	return redcon.AppendError(out, "ERR not implemented")
}

func ERR(code int, message string) *errCommand {
	return &errCommand{
		result: redcon.AppendError(nil, message),
	}
}

func RAW(b []byte) Command {
	return rawCommand(b)
}

type rawCommand []byte

func (c rawCommand) Background(out []byte) []byte {
	return append(out, c...)
}

func (c rawCommand) Invoke(out []byte) []byte {
	return append(out, c...)
}

type errCommand struct {
	Command
	result []byte
}

func (c *errCommand) Invoke(out []byte) []byte {
	return append(out, c.result...)
}

type bgCommand struct {
	Command
}

func (c *bgCommand) Invoke(out []byte) []byte {
	return out
}
