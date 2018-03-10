package server

import (
	"github.com/pointc-io/ipdb/service"

	"github.com/pointc-io/ipdb"
	"github.com/pointc-io/ipdb/redcon/ev"
	"github.com/pointc-io/ipdb/db"
)

type Server struct {
	service.BaseService

	host   string
	path   string
	server *evred.Server
	db     *db.DB
}

func NewServer(host string, path string, eventLoops int) *Server {
	s := &Server{
		host: host,
		path: path,
	}

	s.server = evred.NewServer(host, eventLoops)

	s.BaseService = *service.NewBaseService(butterd.Logger, "server", s)
	return s
}

func (s *Server) OnStart() error {
	s.db = db.NewDB(s.host, s.path)
	err := s.db.Start()
	if err != nil {
		return err
	}

	s.server.SetHandler(s.db)

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
