package main

import (
	"github.com/pointc-io/ipdb/evio"
	"github.com/pointc-io/ipdb/redcon"
	"github.com/pointc-io/ipdb/redcon/ev"
	//"github.com/pointc-io/ipdb/redcon/ev/cluster"
	"github.com/pointc-io/ipdb/db"
	"strings"
	"github.com/spf13/cobra"
	"github.com/pointc-io/ipdb"
	"path/filepath"

	"github.com/garyburd/redigo/redis"
	"encoding/json"
	"log"
)

var bind string
var clusterBind string
var join string
var nodeId string
var dataDir string

var cl *db.Shard
var conn redis.Conn

func main() {
	log.SetOutput(ipdb.Logger)

	var cmdRoot = &cobra.Command{
		Use: ipdb.Name,
		// Default to start as daemon
		Run: func(cmd *cobra.Command, args []string) {
			if nodeId == "" {
				nodeId = bind
			}
			server := evred.NewServer(bind, 1, onCommand)
			err := server.Start()
			if err != nil {
				panic(err)
			}

			cl = db.NewShard(join == "", filepath.Join(dataDir, strings.Replace(nodeId, ":", "", 1)), nodeId)
			cl.Bind = bind
			err = cl.Start()
			//err = cl.Open(join == "", nodeId)
			if err != nil {
				panic(err)
			}

			if join != "" {
				conn, err = redis.Dial("tcp", join)
				if err != nil {
					panic(err)
				}
				//reply, err := conn.Do("CLUSTERJOIN", clusterBind)
				//if err != nil {
				//	panic(err)
				//} else {
				//	fmt.Println(reply)
				//}
			}

			server.Wait()
		},
	}
	cmdRoot.Flags().StringVarP(
		&bind,
		"port",
		"p",
		":15000",
		"Port to run the server on",
	)
	cmdRoot.Flags().StringVarP(
		&clusterBind,
		"clusterport",
		"c",
		":16000",
		"Port to run the RAFT transport on",
	)
	cmdRoot.Flags().StringVarP(
		&join,
		"join",
		"j",
		"",
		"Addr of RAFT node to join",
	)
	cmdRoot.Flags().StringVarP(
		&nodeId,
		"node",
		"n",
		"",
		"Node name",
	)
	cmdRoot.Flags().StringVarP(
		&dataDir,
		"data",
		"d",
		"/Users/clay/.ipdb/cluster",
		"Data directory",
	)

	if err := cmdRoot.Execute(); err != nil {
		panic(err)
	}
}

type Handler struct {
}


func onCommand(conn *evred.RedConn, args [][]byte) (out []byte, action evio.Action) {
	name := strings.ToUpper(string(args[0]))

	switch name {
	default:
		out = redcon.AppendError(out, "ERR not implemented")
	case "GET":
		out = redcon.AppendOK(out)
	case "SET":
		out = redcon.AppendOK(out)

		// Leader addr
	case "CLUSTERLEADER":
		leader, err := cl.Leader()
		if err != nil {
			out = redcon.AppendError(out, "ERR: "+err.Error())
		} else {
			out = redcon.AppendBulkString(out, leader)
		}

	case "CLUSTERSTATE":
		out = redcon.AppendBulkString(out, cl.State().String())

	case "CLUSTERSTATS":
		stats := cl.Stats()
		b, err := json.Marshal(stats)
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendBulkString(out, string(b))
		}

	case "CLUSTERLEAVE":
		err := cl.Leave(string(args[1]), string(args[1]))
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendOK(out)
		}

	case "CLUSTERJOIN":
		err := cl.Join(string(args[1]), string(args[1]))
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendOK(out)
		}

	case "CLUSTERSNAPSHOT":
		err := cl.Snapshot()
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendOK(out)
		}

	case "RAFTSTATE":
	case "RAFTSTATS":
	case "RAFTSTATUS":
	case "RAFTADD":
	case "RAFTREMOVE":

		// RAFT Commands
	case "RAFTSNAPSHOT":
		// Detach
		conn.Detach(cl.HandleInstallSnapshot(args[1]))
		action = evio.Detach
		//out = redcon.AppendOK(out)

	case "RAFTSHRINK":
		err := cl.ShrinkLog()
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendOK(out)
		}

	case "RAFTAPPEND":
		// Second arg is the integer id of the slice.
		// -1 = Shard
		// 0+ = Shard
		//if len(args) != 0
		d, err := cl.AppendEntries(args)
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendBulk(out, d)
		}

	case "RAFTVOTE":
		d, err := cl.RequestVote(args)
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendBulk(out, d)
		}

	case "RAFTINSTALL":

	case "CLUSTERADDSLICE":
	case "CLUSTERDELSLICE":

	case "SHARDJOIN":
	case "SHARDLEAVE":
	case "SHARDSTATS":
	}

	return
}

func raftCommand(conn *evred.RedConn, name string, args [][]byte, pipeline func(conn *evred.RedConn, name string, args [][]byte) (out []byte, action evio.Action)) (out []byte, action evio.Action) {
	switch name {
	default:
		if pipeline == nil {

		} else {
			return pipeline(conn, name, args)
		}
	case "RAFTSTATE":
	case "RAFTSTATS":
	case "RAFTSTATUS":
	case "RAFTADD":
	case "RAFTREMOVE":

	case "RAFTSNAPSHOT":

	case "RAFTSHRINK":
		err := cl.Shrink()
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendOK(out)
		}

	case "RAFTAPPEND":
		// Second arg is the integer id of the slice.
		// -1 = Shard
		// 0+ = Shard
		//if len(args) != 0

	case "RAFTVOTE":

	case "RAFTINSTALL":
	}
	return
}
