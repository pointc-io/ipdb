package command

type Conn interface {
	Close() error
	Handler() Handler
	SetHandler(handler Handler) Handler
	Durability() Durability
}
