package server

import (
	"github.com/pointc-io/sliced/service"

	"github.com/pointc-io/sliced"
	"github.com/pointc-io/sliced/slice"
)

type Server struct {
	service.BaseService

	host   string
	path   string
	server *EvServer
	master *slice.SliceMaster
}

func NewServer(host string, path string, eventLoops int) *Server {
	s := &Server{
		host: host,
		path: path,
	}

	s.server = NewEvServer(host, eventLoops)

	s.BaseService = *service.NewBaseService(sliced.Logger, "server", s)
	return s
}

func (s *Server) OnStart() error {
	s.master = slice.NewMaster(s.host, s.path)
	err := s.master.Start()
	if err != nil {
		return err
	}

	s.server.SetHandler(s.master)

	err = s.server.Start()
	if err != nil {
		s.master.Stop()
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
