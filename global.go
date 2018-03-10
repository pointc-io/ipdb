package butterd

import (
	"os"
	"os/user"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rcrowley/go-metrics"
	"github.com/blang/semver"
)

var Name = "butterd"
var VersionStr = "0.1.0-1" // SemVer
var Version semver.Version
var GIT = ""
var Logger = CLILogger()
var Metrics = metrics.DefaultRegistry
var Path = ""

func init() {
	usr, err := user.Current()
	if err != nil {
		panic(err)
	}

	Path = filepath.Join(usr.HomeDir, ".butterd")
	err = os.MkdirAll(Path, 0700)
	if err != nil {
		panic(err)
	}

	Version, err = semver.Make(VersionStr)
}

func CLILogger() zerolog.Logger {
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
