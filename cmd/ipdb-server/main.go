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

	"github.com/rs/zerolog"
	_ "github.com/rs/zerolog/log"
	"github.com/pointc-io/ipdb"
	"github.com/pointc-io/ipdb/server"
)

var logger zerolog.Logger
var port uint16
var dev bool

func main() {
	// Configure.
	config()

	// Configure as CLI Logger to start
	logger = CLILogger()
	ipdb.Logger = logger

	var cmdStart = &cobra.Command{
		Use:   "start",
		Short: "Starts the app",
		Long:  ``,
		Args:  cobra.MinimumNArgs(0),
		Run:   start,
	}
	cmdStart.Flags().BoolVarP(
		&dev,
		"dev",
		"d",
		false,
		"Run in dev mode.",
	)
	cmdStart.Flags().Uint16VarP(
		&port,
		"port",
		"p",
		7390,
		"Port to run the server on",
	)

	var cmdStatus = &cobra.Command{
		Use:   "status",
		Short: "Prints the current status of " + ipdb.Name,
		Long:  ``,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			pidfile := single.New(ipdb.Name)
			l := pidfile.Lock()
			if l.Success {
				pidfile.Unlock()
				logger.Info().Msg("Daemon is not running")
				return
			} else {
				logger.Info().Msgf("Daemon PID %d", l.Pid)
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
		Use: ipdb.Name,
		// Default to start as daemon
		Run: start,
	}
	cmdRoot.Flags().BoolVarP(
		&dev,
		"dev",
		"d",
		false,
		"Run in dev mode.",
	)
	cmdRoot.Flags().Uint16VarP(
		&port,
		"port",
		"p",
		7390,
		"Port to run the server on",
	)
	cmdRoot.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		Long:  `All software has versions. This is ` + ipdb.Name + `'s`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(fmt.Sprintf("%s %s", ipdb.Name, ipdb.Version))
		},
	})
	cmdRoot.AddCommand(cmdStart, cmdStop, cmdStatus)

	cmdRoot.Execute()
}

// Viper init
func config() {
	viper.SetConfigName("config")                       // name of config file (without extension)
	viper.AddConfigPath(fmt.Sprintf("/etc/%s/", ipdb.Name))  // path to look for the config file in
	viper.AddConfigPath(fmt.Sprintf("$HOME/.%s", ipdb.Name)) // call multiple times to add many search paths
	viper.AddConfigPath(".")                            // optionally look for config in the working directory
	err := viper.ReadInConfig()                         // Find and read the config file
	if err != nil { // incoming errors reading the config file
		//panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}

func start(cmd *cobra.Command, args []string) {
	// Change logger to Daemon Logger
	logger = DaemonLogger()
	ipdb.Logger = logger

	// Ensure only 1 instance through PID lock
	pidfile := single.New(ipdb.Name)
	lockResult := pidfile.Lock()
	if !lockResult.Success {
		logger.Error().Msgf("Process already running PID:%d Host:localhost:%d", lockResult.Pid, lockResult.Port)
		return
	}
	defer pidfile.Unlock()

	// Create, Start and Wait for Daemon to exit
	app := &Daemon{}
	app.BaseService = *service.NewBaseService(logger, "AppService", app)
	app.Start()
	app.Wait()
}

func stop(force bool) {
	// Ensure only 1 instance.
	pidfile := single.New(ipdb.Name)
	l := pidfile.Lock()
	if l.Success {
		pidfile.Unlock()
		logger.Info().Msg("Daemon is not running")
		return
	}

	if l.Pid > 0 {
		process, err := os.FindProcess(l.Pid)
		if err != nil {
			logger.Info().Msgf("Failed to find Daemon PID %d", l.Pid)
			logger.Error().Err(err)
		} else if process == nil {
			logger.Info().Msgf("Failed to find Daemon PID %d", l.Pid)
		} else {
			if force {
				err = process.Kill()
				if err != nil {
					logger.Error().Err(err)
				} else {
					logger.Info().Msg("Daemon was Killed")
				}
			} else {
				logger.Info().Msgf("Sending SIGTERM signal to PID %d", l.Pid)
				err := process.Signal(syscall.SIGTERM)
				if err != nil {
					logger.Error().Err(err)
				} else {
					logger.Info().Msgf("SIGTERM PID %d", l.Pid)
				}
			}
		}
	} else {
		logger.Info().Msg("Daemon is not running")
	}
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

func DaemonLogger() zerolog.Logger {
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

type Daemon struct {
	service.BaseService

	server *server.Server
}

func (d *Daemon) OnStart() error {
	c := make(chan os.Signal, 1)
	slogger := logger.With().Str("Logger", "os.signal").Logger()

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

	d.server = server.NewServer(logger, "", int(port))
	d.server.Start()

	return nil
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
