package server

import "github.com/pointc-io/ipdb/action"

func (c *dbConn) parseCommand(name string, args [][]byte) action.Command {
	switch name {
	case "SLEEP":
		return action.SLEEP(args)

	case "GET":
		return action.GET(args)

	case "SET":
	case "DEL":
	}

	return nil
}
