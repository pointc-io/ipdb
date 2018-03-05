package action

import (
	"github.com/pointc-io/ipdb/redcon"
)

func GET(args [][]byte) Command {
	if len(args) != 2 {
		return &errCommand{result: redcon.AppendError(nil, "ERR wrong number of arguments")}
	} else {
		key := args[1]
		return &getCmd{key: string(key)}
	}
}

type getCmd struct {
	command
	key string
}

func (c *getCmd) Invoke(out []byte) []byte {
	return redcon.AppendBulkString(out, "1")
}
