package runner

import (
	//	"bytes"
	//	"encoding/binary"
	//	"encoding/gob"
	"fmt"
	"strconv"

	pb "github.com/BrobridgeOrg/vibration-api-service/pb"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/syndtr/goleveldb/leveldb"
)

type Database struct {
	name string
	db   *leveldb.DB
}

func OpenDatabase(dbname string) *Database {

	dbpath := fmt.Sprintf("%s/%s", viper.GetString("database.dbpath"), dbname)

	// Open database
	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		log.Error(err)
		return nil
	}

	return &Database{
		name: dbname,
		db:   db,
	}
}

func (database *Database) NewRecord(data *pb.TimerCreation) error {

	key := fmt.Sprintf("%d-%v", data.Timestamp, data.TimerID)
	val, err := proto.Marshal(data.Info)
	if err != nil {
		return err
	}

	// Write to database
	return database.db.Put([]byte(key), val, nil)
}

func (database *Database) NewMetaRecord(data *pb.TimerCreation) error {
	val := strconv.FormatUint(data.Timestamp, 10)
	return database.db.Put([]byte(data.TimerID), []byte(val), nil)
}

func (database *Database) DeleteRecord(key []byte) error {

	// delete record to database
	return database.db.Delete(key, nil)
}

func (database *Database) DeleteMetaRecord(key []byte) error {

	// delete record to database
	return database.DeleteRecord(key)
}
