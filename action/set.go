package action

import (
	"github.com/pointc-io/ipdb/redcon"
	"github.com/pointc-io/ipdb/redcon/ev"
)

func SET(args [][]byte) evred.Command {
	if len(args) != 3 {
		return &evred.ErrCmd{Result: redcon.AppendError(nil, "ERR wrong number of arguments")}
	} else {
		return &setCmd{key: string(args[1]), value: string(args[2])}
	}
}

type setCmd struct {
	evred.Cmd
	key   string
	value string
}

func (c *setCmd) Invoke(out []byte) []byte {
	return redcon.AppendOK(out)
}
