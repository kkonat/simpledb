package simpledb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	dbPath           = "./db"
	dbExt            = ".json"
	MemCacheMaxItems = 100
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type DbItemID int32

// type Hashmap map[int64][]DbItemID
type OffsetsData map[DbItemID]int64

type Cache[T any] struct {
	data     map[DbItemID]*DbItem[T]
	queue    []DbItemID
	requests uint64
	hits     uint64
}

func (m Cache[T]) hitRate() float64 {
	if m.requests > 0 {
		return float64(m.hits) / float64(m.requests) * 100
	} else {
		return 0
	}
}

type SimpleDb[T any] struct {
	infoFilePath string   `json:"-"`
	dataFilePath string   `json:"-"`
	dataFile     *os.File `json:"-"`

	Cache[T]
	ItemCounter DbItemID
	CurrOffset  int64
	Offsets     OffsetsData

	// hashes  Hashmap
}
type DbItem[T any] struct {
	Id       DbItemID
	Data     T
	LastUsed int64
	delete   bool
	// hash 		 int64
}

func init() {
}

func Connect[T any](filename string) (db *SimpleDb[T], err error) {

	dbDataFile := filepath.Clean(filename)
	dir, file := filepath.Split(dbDataFile)
	infoFilePath := filepath.Join(dbPath, dir, "info-"+file+dbExt)
	dataFilePath := filepath.Join(dbPath, dir, "data-"+file+dbExt)

	if _, err = os.Stat(dbPath); err != nil {
		os.Mkdir(dbPath, 0700)
	}
	if _, err = os.Stat(infoFilePath); err == nil {
		db, err = readDbInfo[T](infoFilePath) // read existing database
		if err != nil {
			return
		}
	} else {
		db = &SimpleDb[T]{} // create new database
		db.Offsets = make(OffsetsData)
	}
	db.Cache.data = make(map[DbItemID]*DbItem[T])
	db.queue = make([]DbItemID, 0)

	db.dataFile, err = openDbFile(dataFilePath) // open data file for reading and writing
	if err != nil {
		return nil, fmt.Errorf("error opening database file: %w", err)
	}
	db.infoFilePath = infoFilePath
	db.dataFilePath = dataFilePath
	return
}

func (db *SimpleDb[T]) Kill(dbName string) (err error) {
	_, file := filepath.Split(db.dataFilePath)
	name := strings.Split(file, ".")[0]
	if "data-"+dbName != name {
		return errors.New("security check failed: invalid db name provided")
	}
	db.dataFile.Close()
	db.Cache.data = nil
	db.queue = nil
	if err = os.Remove(db.dataFilePath); err != nil {
		return fmt.Errorf("error removing datafile: %w", err)
	}
	if err = os.Remove(db.infoFilePath); err != nil {
		return fmt.Errorf("error removing infofile: %w", err)
	}
	return
}

func (db *SimpleDb[T]) Append(itemData *T) (id DbItemID, err error) {
	var (
		mtx          sync.Mutex
		data         []byte
		bytesWritten int
	)

	mtx.Lock()
	defer mtx.Unlock()

	item := DbItem[T]{
		Id:       db.genNewId(),
		LastUsed: time.Now().Unix(),
		Data:     *itemData,
	}

	if data, err = json.Marshal(item); err != nil {
		return 0, fmt.Errorf("error marshalling: %w", err)
	}

	// write next data chunk
	bytesWritten = 0

	if len(data) > 65535 {
		panic("data size max. 65535")
	}
	var extdata []byte = make([]byte, 0)
	extdata = binary.LittleEndian.AppendUint16(extdata, uint16(len(data)))
	extdata = append(extdata, data...) // combine this int16  with data

	// write everything at once (pretending to be atomic)
	if bytesWritten, err = db.dataFile.Write(extdata); err != nil {
		return 0, fmt.Errorf("error writing datafile: %w", err)
	}

	db.Offsets[item.Id] = db.CurrOffset
	db.CurrOffset += int64(bytesWritten)
	db.addToMemCache(&item)
	return item.Id, nil
}

func (db *SimpleDb[T]) Get(id DbItemID) (rd *T, err error) {
	// check if that object has ever been created
	seek, ok := db.Offsets[id]
	if !ok {
		return nil, errors.New("item not found in the database")
	}

	if object, ok := db.getFromMemCache(id); ok {
		return &(object.Data), nil
	}

	// if object is not in the mem cache, read it from the database file
	db.dataFile.Seek(seek, 0)

	var l []byte = []byte{0, 0} // two bytes
	var n int
	if n, err = db.dataFile.Read(l); err != nil || n != 2 {
		return nil, fmt.Errorf("error reading datafile: %w", err)
	}

	itemLen := binary.LittleEndian.Uint16(l)

	var data []byte = make([]byte, itemLen)
	if n, err = db.dataFile.Read(data); err != nil || n != int(itemLen) {
		return nil, fmt.Errorf("error reading datafile: %w", err)
	}

	newData := new(DbItem[T])
	if err = unmarshalAny(data, newData); err != nil {
		return nil, fmt.Errorf("error unmarshalling: %w", err)
	}
	db.addToMemCache(newData)
	return &newData.Data, err
}

func (db *SimpleDb[T]) Close() (err error) {
	var data []byte

	db.dataFile.Close()

	if data, err = json.Marshal(db); err != nil {
		return fmt.Errorf("error marshalling infofile: %w", err)
	}
	if err = os.WriteFile(db.infoFilePath, data, 0644); err != nil {
		return fmt.Errorf("error saving infofile: %w", err)
	}
	return
}

func readDbInfo[T any](file string) (db *SimpleDb[T], err error) {
	var data []byte
	if data, err = os.ReadFile(file); err != nil {
		return nil, fmt.Errorf("error reading dBfile: %w", err)
	}
	db = &SimpleDb[T]{}
	err = json.Unmarshal(data, db)
	return
}

func (db *SimpleDb[T]) addToMemCache(item *DbItem[T]) {

	if len(db.Cache.queue) == MemCacheMaxItems {
		delete(db.Cache.data, db.Cache.queue[0])
		db.Cache.queue = db.queue[1:]
	}
	db.Cache.data[item.Id] = item
	db.Cache.queue = append(db.Cache.queue, item.Id)
}
func (db *SimpleDb[T]) getFromMemCache(id DbItemID) (item *DbItem[T], ok bool) {
	item, ok = db.Cache.data[id]
	db.Cache.requests++
	if ok {
		db.Cache.hits++
	}
	return
}

func (db *SimpleDb[T]) genNewId() (id DbItemID) {
	id = db.ItemCounter
	db.ItemCounter++
	return
}

func unmarshalAny[T any](bytes []byte, out *T) error {
	if err := json.Unmarshal(bytes, out); err != nil {
		return err
	}
	return nil
}

// Helper functions
func openDbFile(path string) (file *os.File, err error) {
	file, err = os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	return
}
