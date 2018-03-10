package action

import (
	"github.com/pointc-io/ipdb"
	"github.com/pointc-io/ipdb/redcon"
	"github.com/pointc-io/ipdb/redcon/ev"
)

func VERSION() evred.Command {
	return evred.RAW(redcon.AppendBulkString(nil, butterd.VersionStr))
}
