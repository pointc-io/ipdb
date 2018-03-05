package action

import (
	"github.com/pointc-io/ipdb/redcon"
)

func SET(args [][]byte) Command {
	if len(args) != 3 {
		return &errCommand{result: redcon.AppendError(nil, "ERR wrong number of arguments")}
	} else {
		return &setCmd{key: string(args[1]), value: string(args[2])}
	}
}

type setCmd struct {
	command
	key   string
	value string
}

func (c *setCmd) Invoke(out []byte) []byte {
	return redcon.AppendOK(out)
}
