package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/pointc-io/ipdb/pid"
	"github.com/pointc-io/ipdb/service"

	//_ "go.uber.org/automaxprocs"

	_ "github.com/rs/zerolog/log"
	"github.com/pointc-io/ipdb"

	"runtime"
	"github.com/pointc-io/ipdb/server"
	"os/user"
	"path/filepath"
	"github.com/rs/zerolog"
	"time"
)

var port uint16
var console bool
var loops int
var path string
var loglevel int
var singlemode bool

func main() {
	// Configure.
	config()

	var cmdStart = &cobra.Command{
		Use:   "start",
		Short: "Starts the app",
		Long:  ``,
		Args:  cobra.MinimumNArgs(0),
		Run:   start,
	}
	configureStart(cmdStart)

	var cmdStatus = &cobra.Command{
		Use:   "status",
		Short: "Prints the current status of " + butterd.Name,
		Long:  ``,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			pidfile := single.New(butterd.Name)
			l := pidfile.Lock()
			if l.Success {
				pidfile.Unlock()
				butterd.Logger.Info().Msg("daemon is not running")
				return
			} else {
				butterd.Logger.Info().Msgf("daemon pid %d", l.Pid)
			}
		},
	}

	var force bool
	var cmdStop = &cobra.Command{
		Use:   "stop",
		Short: "Stops the daemon process if it's running",
		Long:  `Determines the daemon PID from the daemon pid lock file and sends a SIGTERM signal if running.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			stop(force)
		},
	}
	cmdStop.Flags().BoolVarP(
		&force,
		"force",
		"f",
		false,
		"Force stop the daemon process. KILL the process.",
	)

	var cmdRoot = &cobra.Command{
		Use: butterd.Name,
		// Default to start as daemon
		Run: start,
	}
	configureStart(cmdRoot)
	cmdRoot.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Long:  `All software has versions. This is ` + butterd.Name + `'s`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(fmt.Sprintf("%s %s", butterd.Name, butterd.VersionStr))
		},
	})
	cmdRoot.AddCommand(cmdStart, cmdStop, cmdStatus)

	// Let's get started!
	cmdRoot.Execute()
}

func configureStart(cmd *cobra.Command) {
	cmd.Flags().BoolVarP(
		&console,
		"console",
		"c",
		false,
		"Format logger for the console",
	)
	cmd.Flags().Uint16VarP(
		&port,
		"port",
		"p",
		7390,
		"Port to run the server on",
	)
	cmd.Flags().IntVarP(
		&loops,
		"eventloops",
		"l",
		runtime.NumCPU()/2,
		"Number of EventLoops to start",
	)
	cmd.Flags().IntVarP(
		&loglevel,
		"loglevel",
		"v",
		int(zerolog.InfoLevel),
		"Log level",
	)
	cmd.Flags().StringVarP(
		&path,
		"dir",
		"d",
		defaultPath(),
		"Root data directory",
	)
	cmd.Flags().BoolVarP(
		&singlemode,
		"singlemode",
		"s",
		false,
		"Start the cluster in single mode",
	)
}

func defaultPath() string {
	usr, err := user.Current()
	if err == nil {
		return ""
	}
	return filepath.Join(usr.HomeDir, ".butterd")
}

// Viper init
func config() {
	viper.SetConfigName("config")                               // name of config file (without extension)
	viper.AddConfigPath(fmt.Sprintf("/etc/%s/", butterd.Name))  // path to look for the config file in
	viper.AddConfigPath(fmt.Sprintf("$HOME/.%s", butterd.Name)) // call multiple times to add many search paths
	viper.AddConfigPath(".")                                    // optionally look for config in the working directory
	err := viper.ReadInConfig()                                 // Find and read the config file
	if err != nil { // incoming errors reading the config file
		//panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}

func start(cmd *cobra.Command, args []string) {
	// Change logger to Daemon Logger
	butterd.Logger = butterd.DaemonLogger(console)

	zerolog.SetGlobalLevel(zerolog.Level(loglevel))
	zerolog.TimeFieldFormat = time.RFC822Z

	if !console {
		// Ensure only 1 instance through PID lock
		pidfile := single.New(butterd.Name)
		lockResult := pidfile.Lock()
		if !lockResult.Success {
			butterd.Logger.Error().Msgf("process already running pid:%d -- localhost:%d", lockResult.Pid, lockResult.Port)
			return
		}
		defer pidfile.Unlock()
	}

	// Create, Start and Wait for Daemon to exit
	app := &Daemon{}
	app.BaseService = *service.NewBaseService(butterd.Logger, "daemon", app)
	err := app.Start()
	if err != nil {
		butterd.Logger.Error().Err(err)
		return
	}
	app.Wait()
}

func stop(force bool) {
	// Ensure only 1 instance.
	pidfile := single.New(butterd.Name)
	l := pidfile.Lock()
	if l.Success {
		pidfile.Unlock()
		butterd.Logger.Info().Msg("daemon is not running")
		return
	}

	if l.Pid > 0 {
		process, err := os.FindProcess(l.Pid)
		if err != nil {
			butterd.Logger.Info().Msgf("failed to find daemon pid %d", l.Pid)
			butterd.Logger.Error().Err(err)
		} else if process == nil {
			butterd.Logger.Info().Msgf("failed to find daemon pid %d", l.Pid)
		} else {
			if force {
				butterd.Logger.Info().Msgf("killing daemon pid %d", l.Pid)
				err = process.Kill()
				if err != nil {
					butterd.Logger.Error().Err(err)
				} else {
					butterd.Logger.Info().Msg("daemon was killed")
				}
			} else {
				butterd.Logger.Info().Msgf("sending SIGTERM signal to pid %d", l.Pid)
				err := process.Signal(syscall.SIGTERM)
				if err != nil {
					butterd.Logger.Error().Err(err)
				} else {
					butterd.Logger.Info().Msgf("SIGTERM pid %d", l.Pid)
				}
			}
		}
	} else {
		butterd.Logger.Info().Msg("daemon is not running")
	}
}

type Daemon struct {
	service.BaseService

	server *server.Server
}

func (d *Daemon) OnStart() error {
	if path != ":memory:" {
		err := os.MkdirAll(path, 0700)
		if err != nil {
			d.Logger.Error().Err(err)
			return err
		}
	}
	// Handle os signals.
	c := make(chan os.Signal, 1)
	slogger := butterd.Logger.With().Str("logger", "os.signal").Logger()
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		s := <-c
		// Log signal
		slogger.Info().Msgf("%s", s)

		// Stop app
		d.Stop()

		switch {
		default:
			os.Exit(-1)
		case s == syscall.SIGHUP:
			os.Exit(1)
		case s == syscall.SIGINT:
			os.Exit(2)
		case s == syscall.SIGQUIT:
			os.Exit(3)
		case s == syscall.SIGTERM:
			os.Exit(0xf)
		}
	}()

	// Start Server.
	d.server = server.NewServer(fmt.Sprintf("localhost:%d", int(port)), path, loops)
	err := d.server.Start()

	return err
}

func (d *Daemon) OnStop() {
	err := d.server.Stop()
	if err != nil {
		d.Logger.Error().Err(err)
	}
}

//func (c *Daemon) watchOutOfMemory() {
//	t := time.NewTicker(time.Second * 2)
//	defer t.Stop()
//	var mem runtime.MemStats
//	for range t.C {
//		func() {
//			if c.stopWatchingMemory.on() {
//				return
//			}
//			oom := c.outOfMemory.on()
//			//if c.config.maxMemory() == 0 {
//			//	if oom {
//			//		c.outOfMemory.set(false)
//			//	}
//			//	return
//			//}
//			if oom {
//				runtime.GC()
//			}
//			runtime.ReadMemStats(&mem)
//			c.outOfMemory.set(mem.HeapAlloc > c.maxMemory)
//		}()
//	}
//}
