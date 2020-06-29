package app

import (
	"strconv"
	"time"
	"vibration-runner/app/eventbus"
	app "vibration-runner/app/interface"
	"vibration-runner/app/signalbus"
	"vibration-runner/services/runner"

	nats "github.com/nats-io/nats.go"
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
	isReady   bool
}

func CreateApp() *App {

	// Genereate a unique ID for instance
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		return nil
	}

	idStr := strconv.FormatUint(id, 16)

	a := &App{
		id:    id,
		flake: flake,
	}
	a.eventbus = eventbus.CreateConnector(
		viper.GetString("event_store.host"),
		viper.GetString("event_store.cluster_id"),
		idStr,
		func(natsConn *nats.Conn) {

			for {
				log.Warn("re-connect to event server")

				// Connect to NATS Streaming
				err := a.eventbus.Connect()
				if err != nil {
					log.Error("Failed to connect to event server")
					time.Sleep(time.Duration(1) * time.Second)
					continue
				}

				a.isReady = true

				break
			}

		},
		func(natsConn *nats.Conn) {
			a.isReady = false
		},
	)
	a.signalbus = signalbus.CreateConnector(
		viper.GetString("event_store.host"),
		idStr,
		func(natsConn *nats.Conn) {

			for {
				log.Warn("re-connect to signal server")

				// Connect to NATS Streaming
				err := a.signalbus.Connect()
				if err != nil {
					log.Error("Failed to connect to signal server")
					time.Sleep(time.Duration(1) * time.Second)
					continue
				}

				a.isReady = true

				break
			}

		},
		func(natsConn *nats.Conn) {
			a.isReady = false
		},
	)

	return a

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
