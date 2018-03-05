package action

import (
	"github.com/pointc-io/ipdb"
	"github.com/pointc-io/ipdb/redcon"
)

func VERSION() Command {
	return RAW(redcon.AppendBulkString(nil, ipdb.Version))
}
