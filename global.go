package sliced

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"

	"github.com/blang/semver"
	"github.com/rcrowley/go-metrics"
	"github.com/rs/zerolog"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an value or idx is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an idx already exists in the database.
	ErrIndexExists = errors.New("idx exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is returned when post-loading data from an database
	// not opened with Open(":memory:").
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")

	ErrLogNotShrinkable    = errors.New("log not shrinkable")
	ErrShardNotExists      = errors.New("shard not exists")
	ErrNotLeader           = errors.New("not leader")
	ErrNotLeaderOrFollower = errors.New("not leader or follower")
)

var Name = "sliced"
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

	Path = filepath.Join(usr.HomeDir, ".sliced")
	err = os.MkdirAll(Path, 0700)
	if err != nil {
		panic(err)
	}

	Version, err = semver.Make(VersionStr)

	PluginCtx = &context{}
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
