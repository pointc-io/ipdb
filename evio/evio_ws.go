package evio

import "github.com/rcrowley/go-metrics"

type WebSocketServer struct {
	connsCreated *metrics.Counter
}

func NewWebSocketServer() *WebSocketServer {
	return &WebSocketServer{}
}
