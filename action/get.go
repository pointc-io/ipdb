package action

import (
	"github.com/pointc-io/ipdb/redcon"
	"github.com/pointc-io/ipdb/redcon/ev"
)

func GET(args [][]byte) evred.Command {
	if len(args) != 2 {
		return &evred.ErrCmd{Result: redcon.AppendError(nil, "ERR wrong number of arguments")}
	} else {
		key := args[1]
		return &getCmd{key: string(key)}
	}
}

type getCmd struct {
	evred.Cmd
	key string
}

func (c *getCmd) Invoke(out []byte) []byte {
	return redcon.AppendBulkString(out, "1")
}
