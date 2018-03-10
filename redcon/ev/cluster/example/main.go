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
	"github.com/garyburd/redigo/redis"
	"encoding/json"
	"log"
)

var bind string
var clusterBind string
var join string
var nodeId string
var dataDir string

var shard *db.Shard
var conn redis.Conn

func main() {
	log.SetOutput(butterd.Logger)

	var cmdRoot = &cobra.Command{
		Use: butterd.Name,
		// Default to start as daemon
		Run: func(cmd *cobra.Command, args []string) {
			if nodeId == "" {
				nodeId = bind
			}
			server := evred.NewServer(bind, 1)
			server.SetHandler(onCommand)
			err := server.Start()
			if err != nil {
				panic(err)
			}

			// filepath.Join(dataDir, strings.Replace(nodeId, ":", "", 1)),
			shard = db.NewShard(0, join == "", ":memory:", nodeId)
			shard.Bind = bind
			err = shard.Start()
			//err = shard.Open(join == "", nodeId)
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

func onCommand(conn *evred.Conn, args [][]byte) (out []byte, action evio.Action) {
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
		leader, err := shard.Leader()
		if err != nil {
			out = redcon.AppendError(out, "ERR: "+err.Error())
		} else {
			out = redcon.AppendBulkString(out, leader)
		}

	case "CLUSTERSTATE":
		out = redcon.AppendBulkString(out, shard.State().String())

	case "CLUSTERSTATS":
		stats := shard.Stats()
		b, err := json.Marshal(stats)
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendBulkString(out, string(b))
		}

	case "CLUSTERLEAVE":
		err := shard.Leave(string(args[1]), string(args[1]))
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendOK(out)
		}

	case "CLUSTERJOIN":
		err := shard.Join(string(args[1]), string(args[1]))
		if err != nil {
			out = redcon.AppendError(out, err.Error())
		} else {
			out = redcon.AppendOK(out)
		}

	case "CLUSTERSNAPSHOT":
		err := shard.Snapshot()
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
		conn.Detach(shard.HandleInstallSnapshot(args[1]))
		action = evio.Detach
		//out = redcon.AppendOK(out)

	case "RAFTSHRINK":
		err := shard.ShrinkLog()
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
		out, _ = shard.AppendEntries(out, args)

	case "RAFTVOTE":
		out, _ = shard.RequestVote(out, args)

	case "RAFTINSTALL":

	case "CLUSTERADDSLICE":
	case "CLUSTERDELSLICE":

	case "SHARDJOIN":
	case "SHARDLEAVE":
	case "SHARDSTATS":
	}

	return
}

func raftCommand(conn *evred.Conn, name string, args [][]byte, pipeline func(conn *evred.Conn, name string, args [][]byte) (out []byte, action evio.Action)) (out []byte, action evio.Action) {
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
		err := shard.Shrink()
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
