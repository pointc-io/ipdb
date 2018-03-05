package action

import (
	"github.com/pointc-io/ipdb/worker"
	"github.com/pointc-io/ipdb/redcon"
	"context"
)

var NeedsDispatch = []byte{}

type Command interface {
	worker.WorkerJob

	// Invoke happens on the EventLoop
	Invoke() []byte
}

type command struct {
	Command
}

func (c *command) Invoke() []byte {
	return redcon.AppendError(nil, "ERR not implemented")
}

func (c *command) Run(ctx context.Context) {
	redcon.AppendError(nil, "ERR not implemented")
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

func (c rawCommand) Run(ctx context.Context) {
}

func (c rawCommand) Invoke() []byte {
	return c
}

type errCommand struct {
	Command
	result []byte
}

func (c *errCommand) Invoke() []byte {
	return c.result
}

type bgCommand struct {
	Command
}

func (c *bgCommand) Invoke() []byte {
	return NeedsDispatch
}
