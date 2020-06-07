package runner

import (
	//	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	nats "github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb/util"

	pb "github.com/BrobridgeOrg/timer-api-service/pb"
	app "timer-runner/app/interface"
)

type Service struct {
	app                app.AppImpl
	dbMgr              *DatabaseManager
	createEventChannel *nats.Subscription
	deleteEventChannel *nats.Subscription
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

	// listen queue
	// subscribe atomic-clock
	signalBus := service.app.GetSignalBus()
	service.tickerChannel, _ = signalBus.Watch(
		"timer.ticker",
		func(m *nats.Msg) {
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
	service.createEventChannel, _ = signalBus.QueueSubscribe(
		"timer.timerCreated",
		"create.transaction",
		func(m *nats.Msg) {
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
	service.deleteEventChannel, _ = signalBus.Watch(
		"timer.timerDeleted",
		func(m *nats.Msg) {
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
	//eventBus := service.app.GetEventBus()

	//Unsubscribe
	signalBus.Unsubscribe(service.createEventChannel)
	signalBus.Unsubscribe(service.deleteEventChannel)
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
		//data.Info = &dataInfo

		timestamp, timerID := parseKey(iter.Key())
		//data.Timestamp = timestamp
		//data.TimerID = timerID

		data := pb.TimerCreation{
			Timestamp: timestamp,
			TimerID:   timerID,
			Info:      &dataInfo,
		}
		publishData, _ := proto.Marshal(&data)

		// publish event data to queue
		signalBus.Emit("timer.timerCreated", publishData)
		//eventBus.Emit("timer.timerCreated", iter.Value())

		// Delete data
		db.DeleteRecord(iter.Key())
		dbMeta.DeleteMetaRecord([]byte(timerID))

	}
	iter.Release()

	return nil
}
