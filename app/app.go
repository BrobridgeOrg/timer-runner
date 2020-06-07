package app

import (
	"strconv"
	"timer-runner/app/eventbus"
	app "timer-runner/app/interface"
	"timer-runner/app/signalbus"
	"timer-runner/services/runner"

	log "github.com/sirupsen/logrus"
	"github.com/sony/sonyflake"
	"github.com/spf13/viper"
)

type App struct {
	id        uint64
	flake     *sonyflake.Sonyflake
	signalbus *signalbus.SignalBus
	eventbus  *eventbus.EventBus
	runner    *runner.Service
}

func CreateApp() *App {

	// Genereate a unique ID for instance
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		return nil
	}

	idStr := strconv.FormatUint(id, 16)

	return &App{
		id:    id,
		flake: flake,
		eventbus: eventbus.CreateConnector(
			viper.GetString("event_store.host"),
			viper.GetString("event_store.cluster_id"),
			idStr,
		),
		signalbus: signalbus.CreateConnector(
			viper.GetString("event_store.host"),
			idStr,
		),
	}
}

func (a *App) Init() error {

	log.WithFields(log.Fields{
		"a_id": a.id,
	}).Info("Starting application")

	// Connect to signal server
	err := a.signalbus.Connect()
	if err != nil {
		return err
	}

	// Connect to event server
	err = a.eventbus.Connect()
	if err != nil {
		return err
	}

	return nil
}

func (a *App) Uninit() {

	a.runner.Stop()
}

func (a *App) Run() error {

	a.runner = runner.CreateService(app.AppImpl(a))
	a.runner.Start()

	return nil
}

func (a *App) GetSignalBus() app.SignalBusImpl {
	return app.SignalBusImpl(a.signalbus)
}

func (a *App) GetEventBus() app.EventBusImpl {
	return app.EventBusImpl(a.eventbus)
}
