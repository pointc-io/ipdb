package action

import (
	"strconv"
	"time"

	"github.com/pointc-io/sliced/redcon"
	"github.com/pointc-io/sliced/redcon/ev"
)

func SLEEP(args [][]byte) evred.Command {
	// Validate.
	if len(args) != 2 {
		return evred.ERR(0, "ERR wrong number of arguments for 'SLEEP' command")
	} else {
		seconds, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return evred.ERR(0, "ERR invalid seconds")
		} else {
			return &sleepCmd{
				seconds: seconds,
			}
		}
	}
}

type sleepCmd struct {
	evred.BgCmd
	seconds int
}

func (c *sleepCmd) Background(out []byte) []byte {
	time.Sleep(time.Duration(c.seconds) * time.Second)
	return redcon.AppendOK(out)
}
