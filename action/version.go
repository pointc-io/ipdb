package action

import (
	"github.com/pointc-io/sliced"
	"github.com/pointc-io/sliced/redcon"
	"github.com/pointc-io/sliced/redcon/ev"
)

func VERSION() evred.Command {
	return evred.RAW(redcon.AppendBulkString(nil, sliced.VersionStr))
}
