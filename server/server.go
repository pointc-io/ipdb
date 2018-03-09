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
	"sort"
	"strings"
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
		path: path,
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
	name := strings.ToUpper(string(args[0]))

	switch name {
	default:
		out = redcon.AppendError(out, "ERR command " + name + " not found")
	case "RAFTAPPEND":
		if len(args) < 3 {
			out = redcon.AppendError(out, "ERR 2 parameters expected")
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
			out = redcon.AppendError(out, "ERR 2 parameters expected")
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
			out = redcon.AppendError(out, "ERR 2 parameters expected")
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
		if len(args) < 2 {
			out = redcon.AppendError(out, "ERR 1 parameter expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				out = redcon.AppendError(out, "ERR invalid int param for shard id")
			} else {
				shard := s.db.Shard(id)
				if shard == nil {
					out = redcon.AppendError(out, fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					err := shard.ShrinkLog()
					if err != nil {
						out = redcon.AppendError(out, "ERR "+err.Error())
					} else {
						out = redcon.AppendOK(out)
					}
				}
			}
		}
	case "RAFTSTATS":
		if len(args) < 2 {
			out = redcon.AppendError(out, "ERR 1 parameter expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				out = redcon.AppendError(out, "ERR invalid int param for shard id")
			} else {
				shard := s.db.Shard(id)
				if shard == nil {
					out = redcon.AppendError(out, fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					stats := shard.Stats()
					keys := make([]string, 0, len(stats))
					for key := range stats {
						keys = append(keys, key)
					}
					sort.Strings(keys)
					out = redcon.AppendArray(out, len(keys)*2)
					for _, key := range keys {
						out = redcon.AppendBulkString(out, key)
						out = redcon.AppendBulkString(out, stats[key])
					}
				}
			}
		}
	case "RAFTSTATE":
		if len(args) < 2 {
			out = redcon.AppendError(out, "ERR 1 parameter expected")
		} else {
			id, err := strconv.Atoi(string(args[1]))
			if err != nil {
				out = redcon.AppendError(out, "ERR invalid int param for shard id")
			} else {
				shard := s.db.Shard(id)
				if shard == nil {
					out = redcon.AppendError(out, fmt.Sprintf("ERR shard %d not on node", id))
				} else {
					state := shard.State()
					out = redcon.AppendBulkString(out, state.String())
				}
			}
		}

	case "SHARDCOUNT":
		out = redcon.AppendInt(out, 1)
	}
	return
}
