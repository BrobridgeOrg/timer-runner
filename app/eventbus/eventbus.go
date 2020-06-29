package eventbus

import (
	"time"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
)

type EventBus struct {
	host              string
	clusterID         string
	clientName        string
	client            stan.Conn
	reconnectHandler  func(natsConn *nats.Conn)
	disconnectHandler func(natsConn *nats.Conn)
}

func CreateConnector(host string, clusterID string, clientName string, reconnectHandler func(natsConn *nats.Conn), disconnectHandler func(natsConn *nats.Conn)) *EventBus {
	return &EventBus{
		host:              host,
		clusterID:         clusterID,
		clientName:        clientName,
		reconnectHandler:  reconnectHandler,
		disconnectHandler: disconnectHandler,
	}
}

func (eb *EventBus) Connect() error {

	log.WithFields(log.Fields{
		"host":       eb.host,
		"clientName": eb.clientName,
		"clusterID":  eb.clusterID,
	}).Info("Connecting to event server")

	// Connect to queue server
	nc, err := nats.Connect(
		eb.host,
		nats.MaxReconnects(-1),
		nats.PingInterval(10*time.Second),
		nats.MaxPingsOutstanding(3),
		nats.ReconnectHandler(eb.reconnectHandler),
		nats.DisconnectHandler(eb.disconnectHandler),
	)
	if err != nil {
		return err
	}

	sc, err := stan.Connect(eb.clusterID, eb.clientName, stan.NatsConn(nc))
	if err != nil {
		return err
	}

	eb.client = sc

	return nil
}

func (eb *EventBus) Close() {
	eb.client.Close()
}

func (eb *EventBus) Emit(eventName string, data []byte) error {

	if err := eb.client.Publish(eventName, data); err != nil {
		return err
	}

	return nil
}

func (eb *EventBus) On(eventName string, fn func(*stan.Msg)) (stan.Subscription, error) {

	sub, err := eb.client.Subscribe(eventName, fn)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

func (eb *EventBus) QueueSubscribe(channelName string, topic string, fn func(*stan.Msg)) (stan.Subscription, error) {

	sub, err := eb.client.QueueSubscribe(channelName, topic, fn)
	if err != nil {

		return nil, err
	}

	return sub, nil
}

func (eb *EventBus) Unsubscribe(sub stan.Subscription) error {

	// Unsubscribe
	err := sub.Unsubscribe()
	if err != nil {
		return err
	}

	return nil
}
