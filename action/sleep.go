package action

import (
	"github.com/pointc-io/ipdb/redcon"
	"strconv"
	"time"
)

func SLEEP(args [][]byte) Command {
	// Validate.
	if len(args) != 2 {
		return ERR(0, "ERR wrong number of arguments for 'SLEEP' command")
	} else {
		seconds, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return ERR(0, "ERR invalid seconds")
		} else {
			return &sleepCmd{
				seconds: seconds,
			}
		}
	}
}

type sleepCmd struct {
	bgCommand
	seconds int
}

func (c *sleepCmd) Background(out []byte) []byte {
	time.Sleep(time.Duration(c.seconds) * time.Second)
	return redcon.AppendOK(out)
}
