package service

import (
	"errors"
	"fmt"
	"sync/atomic"

	"time"
	"github.com/rs/zerolog"
)

var (
	ErrAlreadyStarted = errors.New("already started")
	ErrAlreadyStopped = errors.New("already stopped")
)

// Service defines a service that can be started, stopped, and reset.
type Service interface {
	// Start the service.
	// If it's already started or stopped, will return an error.
	// If OnStart() returns an error, it's returned by Start()
	Start() error
	OnStart() error

	// Stop the service.
	// If it's already stopped, will return an error.
	// OnStop must never error.
	Stop() error
	OnStop()

	// Reset the service.
	// Panics by default - must be overwritten to enable reset.
	Reset() error
	OnReset() error

	// Return true if the service is running
	IsRunning() bool

	// String representation of the service
	String() string

	SetLogger(logger zerolog.Logger)
}

/*
Classical-inheritance-style service declarations. Services can be started, then
stopped, then optionally restarted.

Users can override the OnStart/OnStop methods. In the absence of errors, these
methods are guaranteed to be called at most once. If OnStart returns an error,
service won't be marked as started, so the user can call Start again.

A call to Reset will panic, unless OnReset is overwritten, allowing
OnStart/OnStop to be called again.

The caller must ensure that Start and Stop are not called concurrently.

It is ok to call Stop without calling Start first.

Typical usage:

	type FooService struct {
		BaseService
		// private fields
	}

	func NewFooService() *FooService {
		fs := &FooService{
			// init
		}
		fs.BaseService = *NewBaseService(log, "FooService", fs)
		return fs
	}

	func (fs *FooService) OnStart() error {
		fs.BaseService.OnStart() // Always call the overridden method.
		// initialize private fields
		// start subroutines, etc.
	}

	func (fs *FooService) OnStop() error {
		fs.BaseService.OnStop() // Always call the overridden method.
		// close/destroy private fields
		// stop subroutines, etc.
	}
*/
type BaseService struct {
	Logger      zerolog.Logger
	name        string
	startedTime time.Time
	started     uint32 // atomic
	stopped     uint32 // atomic
	Quit        chan struct{}

	// The "subclass" of BaseService
	impl Service
}

func NewBaseService(logger zerolog.Logger, name string, impl Service) *BaseService {
	return &BaseService{
		Logger: logger.With().Str("Logger", name).Logger(),
		name:   name,
		Quit:   make(chan struct{}),
		impl:   impl,
	}
}

func (bs *BaseService) SetLogger(l zerolog.Logger) {
	bs.Logger = l
}

// Implements Servce
func (bs *BaseService) Start() error {
	if atomic.CompareAndSwapUint32(&bs.started, 0, 1) {
		if atomic.LoadUint32(&bs.stopped) == 1 {
			bs.Logger.Error().Msgf("Not starting %v -- already stopped", bs.name)
			return ErrAlreadyStopped
		} else {
			bs.Logger.Info().Msg("Starting...")
		}
		started := time.Now()
		bs.startedTime = started
		err := bs.impl.OnStart()
		if err != nil {
			// revert flag
			atomic.StoreUint32(&bs.started, 0)
			return err
		}
		bs.Logger.Info().Dur("Started", time.Now().Sub(started)).Msg("")
		return nil
	} else {
		bs.Logger.Debug().Msgf("Not starting %v -- already started", bs.name)
		return ErrAlreadyStarted
	}
}

// Implements Service
// NOTE: Do not put anything in here,
// that way users don't need to call BaseService.OnStart()
func (bs *BaseService) OnStart() error { return nil }

// Implements Service
func (bs *BaseService) Stop() error {
	if atomic.CompareAndSwapUint32(&bs.stopped, 0, 1) {
		bs.Logger.Info().Msg("Stopping...")
		started := time.Now()
		bs.impl.OnStop()
		close(bs.Quit)
		bs.Logger.Info().
			Dur("StopDuration", time.Now().Sub(started)).
			Dur("Lifespan", time.Now().Sub(bs.startedTime)).
			Msg("Stopped")
		return nil
	} else {
		bs.Logger.Debug().Msgf("Stopping %v (ignoring: already stopped)", bs.name)
		return ErrAlreadyStopped
	}
}

// Implements Service
// NOTE: Do not put anything in here,
// that way users don't need to call BaseService.OnStop()
func (bs *BaseService) OnStop() {}

// Implements Service
func (bs *BaseService) Reset() error {
	if !atomic.CompareAndSwapUint32(&bs.stopped, 1, 0) {
		bs.Logger.Debug().Msgf("Can't reset %v. Not stopped", bs.name)
		return fmt.Errorf("can't reset running %s", bs.name)
	}

	// whether or not we've started, we can reset
	atomic.CompareAndSwapUint32(&bs.started, 1, 0)

	bs.Quit = make(chan struct{})
	return bs.impl.OnReset()
}

// Implements Service
func (bs *BaseService) OnReset() error {
	bs.Logger.Panic().Msg("The service cannot be reset")
	return nil
}

// Implements Service
func (bs *BaseService) IsRunning() bool {
	return atomic.LoadUint32(&bs.started) == 1 && atomic.LoadUint32(&bs.stopped) == 0
}

func (bs *BaseService) Wait() {
	<-bs.Quit
}

// Implements Servce
func (bs *BaseService) String() string {
	return bs.name
}
