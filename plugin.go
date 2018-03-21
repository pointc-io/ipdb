package sliced

func GetContextName() string {
	return PluginCtx.name
}

func SetContextName(name string) {
	PluginCtx.name = name
}

type Context interface {
	Exec(arg string) string
}

type context struct {
	name string
}

func (s *context) Exec(arg string) string {
	return s.name + ": " + arg
}

var PluginCtx *context
