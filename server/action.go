package server

//import (
//	"github.com/pointc-io/ipdb/action"
//	"github.com/tidwall/redcon"
//	"github.com/pointc-io/ipdb/redcon/ev"
//)
//
//func (c *dbConn) parseCommand(name string, args [][]byte) evred.Command {
//	switch name {
//	case "SLEEP":
//		return action.SLEEP(args)
//
//	case "GET":
//		return action.GET(args)
//
//	case "VERSION":
//		return action.VERSION()
//
//	case "SET":
//		return action.SET(args)
//
//	case "EVID":
//		return evred.RAW(redcon.AppendInt(nil, int64(c.ev.id)))
//
//	case "DEL":
//	}
//
//	return nil
//}
