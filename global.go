package ipdb

import (
	"os"
	"sync"
	"os/user"
	"path/filepath"
	"github.com/rs/zerolog"
	"github.com/rcrowley/go-metrics"
)

var Name = "btrd"
var Version = "0.1.0-1" // SemVer
var GIT = ""
var Logger zerolog.Logger = CLILogger()
var Registry = metrics.DefaultRegistry
var Path = ""

var mu sync.Mutex

func init() {
	usr, err := user.Current()
	if err != nil {
		panic(err)
	}
	Path = filepath.Join(usr.HomeDir, ".ipdb")
}

func CLILogger() zerolog.Logger {
	//l := zerolog.New(os.Stdout)
	l := zerolog.New(zerolog.ConsoleWriter{
		Out:     os.Stdout,
		NoColor: false,
	})
	l = l.With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	return l
}

func DaemonLogger(dev bool) zerolog.Logger {
	if dev {
		return CLILogger()
	}

	l := zerolog.New(os.Stdout)
	//l := zerolog.New(zerolog.ConsoleWriter{
	//	Out:     os.Stdout,
	//	NoColor: false,
	//})
	l = l.With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	return l
}
