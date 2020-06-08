package runner

import (
	//	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb/util"

	pb "github.com/BrobridgeOrg/timer-api-service/pb"
	app "timer-runner/app/interface"
)

type Service struct {
	app                app.AppImpl
	dbMgr              *DatabaseManager
	createEventChannel stan.Subscription
	deleteEventChannel stan.Subscription
	tickerChannel      *nats.Subscription
}

func CreateService(a app.AppImpl) *Service {

	dm := CreateDatabaseManager()
	if dm == nil {
		return nil
	}

	// Preparing service
	service := &Service{
		app:   a,
		dbMgr: dm,
	}

	return service
}

func parseKey(key []byte) (uint64, string) {
	keys := strings.Split(string(key), "-")
	timestamp, _ := strconv.ParseUint(keys[0], 10, 64)
	timerID := keys[1]
	return timestamp, timerID
}

func (service *Service) Start() error {

	//Get Database
	db := service.dbMgr.GetDatabase("timerEvent")

	// Get MetaData Database
	dbMeta := service.dbMgr.GetDatabase("timerEventMetadata")

	// get nats
	signalBus := service.app.GetSignalBus()

	// get nats streaming
	eventBus := service.app.GetEventBus()

	// subscribe atomic-clock
	service.tickerChannel, _ = signalBus.Watch(
		"timer.ticker",
		func(m *nats.Msg) {
			log.Info("Trigger timer event ... ")
			// search time's up data
			searchKey := fmt.Sprintf("%v-", string(m.Data))
			iter := db.db.NewIterator(util.BytesPrefix([]byte(searchKey)), nil)
			for iter.Next() {
				// publist data
				signalBus.Emit("timer.timeup", iter.Value())

				_, timerID := parseKey(iter.Key())
				db.DeleteRecord(iter.Key())
				dbMeta.DeleteMetaRecord([]byte(timerID))
			}
			iter.Release()

		},
	)

	// queue subscribe create event
	service.createEventChannel, _ = eventBus.QueueSubscribe(
		"timer.timerCreated",
		"create.transaction",
		func(m *stan.Msg) {
			log.Info("Create timer event  ... ")
			// save to db
			var createEvent pb.TimerCreation
			err := proto.Unmarshal(m.Data, &createEvent)
			if err != nil {
				log.Error(err)
			}

			// insert data
			db.NewRecord(&createEvent)
			// insert metadata
			dbMeta.NewMetaRecord(&createEvent)
		},
	)

	// subscribe delete event
	service.deleteEventChannel, _ = eventBus.On(
		"timer.timerDeleted",
		func(m *stan.Msg) {
			log.Info("Delete timer event ... ")
			// search db date and delete it.
			var deleteEvent pb.TimerDeletion
			err := proto.Unmarshal(m.Data, &deleteEvent)
			if err != nil {
				log.Error(err)
			}
			timestamp, _ := dbMeta.db.Get([]byte(deleteEvent.TimerID), nil)

			dataKey := fmt.Sprintf("%s-%s", string(timestamp), deleteEvent.TimerID)
			db.DeleteRecord([]byte(dataKey))
			dbMeta.DeleteMetaRecord([]byte(deleteEvent.TimerID))
		},
	)

	return nil
}

func (service *Service) Stop() error {
	// clean leveldb's data and send to queue system
	signalBus := service.app.GetSignalBus()
	eventBus := service.app.GetEventBus()

	//Unsubscribe
	eventBus.Unsubscribe(service.createEventChannel)
	eventBus.Unsubscribe(service.deleteEventChannel)
	signalBus.Unsubscribe(service.tickerChannel)

	// Get event data
	// Iterate over database content
	log.Info("Release Event data ... ")

	db := service.dbMgr.GetDatabase("timerEvent")
	dbMeta := service.dbMgr.GetDatabase("timerEventMetadata")

	iter := db.db.NewIterator(nil, nil)
	for iter.Next() {

		//process data
		var dataInfo pb.TimerInfo
		proto.Unmarshal(iter.Value(), &dataInfo)

		timestamp, timerID := parseKey(iter.Key())

		data := pb.TimerCreation{
			Timestamp: timestamp,
			TimerID:   timerID,
			Info:      &dataInfo,
		}
		publishData, _ := proto.Marshal(&data)

		// publish event data to queue
		signalBus.Emit("timer.timerCreated", publishData)

		// Delete data
		db.DeleteRecord(iter.Key())
		dbMeta.DeleteMetaRecord([]byte(timerID))

	}
	iter.Release()

	return nil
}
