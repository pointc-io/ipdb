package server

import (
	"github.com/pointc-io/ipdb/evio"
	"github.com/pointc-io/ipdb/service"

	"github.com/pointc-io/ipdb"
	"github.com/pointc-io/ipdb/redcon/ev"
	"github.com/pointc-io/ipdb/db"
	"github.com/tidwall/redcon"
	"strconv"
	"fmt"
)

type Server struct {
	service.BaseService

	host   string
	path   string
	server *evred.EvServer
	db     *db.DB
}

func NewServer(host string, path string, eventLoops int) *Server {
	s := &Server{
		host: host,
	}

	s.server = evred.NewServer(host, eventLoops, s.onCommand)

	s.BaseService = *service.NewBaseService(ipdb.Logger, "server", s)
	return s
}

func (s *Server) OnStart() error {
	s.db = db.NewDB(s.host, s.path)
	err := s.db.Start()
	if err != nil {
		return err
	}

	err = s.server.Start()
	if err != nil {
		s.db.Stop()
		return err
	}

	return nil
}

func (s *Server) OnStop() {
	err := s.server.Stop()
	if err != nil {
		s.Logger.Error().Err(err)
	}
}

func (s *Server) onCommand(conn *evred.RedConn, args [][]byte) (out []byte, action evio.Action) {
	name := string(args[0])

	switch name {
	case "RAFTAPPEND":
		if len(args) < 3 {
			out = redcon.AppendError(out, "ERR 3 parameters expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				out = redcon.AppendError(out, "ERR invalid int param for shard id")
			} else {
				shard := s.db.Shard(id)
				if shard == nil {
					out = redcon.AppendError(out, fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					out, _ = shard.AppendEntries(out, args)
				}
			}
		}
	case "RAFTVOTE":
		if len(args) < 3 {
			out = redcon.AppendError(out, "ERR 3 parameters expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				out = redcon.AppendError(out, "ERR invalid int param for shard id")
			} else {
				shard := s.db.Shard(id)
				if shard == nil {
					out = redcon.AppendError(out, fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					out, _ = shard.RequestVote(out, args)
				}
			}
		}

	case "RAFTINSTALL":
		if len(args) < 3 {
			out = redcon.AppendError(out, "ERR 3 parameters expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				out = redcon.AppendError(out, "ERR invalid int param for shard id")
			} else {
				shard := s.db.Shard(id)
				if shard == nil {
					out = redcon.AppendError(out, fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					conn.Detach(shard.HandleInstallSnapshot(args[2]))
					action = evio.Detach
				}
			}
		}

	case "RAFTSHRINK":
	}
	return
}
