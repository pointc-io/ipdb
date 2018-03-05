package action

import (
	"github.com/pointc-io/ipdb/redcon"
)

func SET(args [][]byte) Command {
	if len(args) != 3 {
		return &errCommand{result: redcon.AppendError(nil, "ERR wrong number of arguments")}
	} else {
		key := args[1]
		return &setCmd{key: string(key)}
	}
}

type setCmd struct {
	command
	key string
}

func (c *setCmd) Invoke() []byte {
	return redcon.AppendOK(nil)
}
