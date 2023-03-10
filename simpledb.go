package simpledb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
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

type MemCache[T any] struct {
	data     map[DbItemID]*DbItem[T]
	queue    []DbItemID
	requests uint64
	hits     uint64
}

func (m MemCache[T]) hitRate() (float64, error) {
	if m.requests > 0 {
		return float64(m.hits) / float64(m.requests), nil
	} else {
		return 0, errors.New("No requests yet")
	}
}

type SimpleDb[T any] struct {
	infoFilePath string   `json:"-"`
	dataFilePath string   `json:"-"`
	dataFile     *os.File `json:"-"`

	MemCache[T]
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
		log.Info(infoFilePath + " database already exists\nReading in\n")
		db, err = readDbInfo[T](infoFilePath) // read existing database
		if err != nil {
			return
		}
	} else {
		db = &SimpleDb[T]{} // create new database
		db.Offsets = make(OffsetsData)
	}
	db.MemCache.data = make(map[DbItemID]*DbItem[T])
	db.queue = make([]DbItemID, 0)

	db.dataFile, err = openDbFile(dataFilePath) // open data file for reading and writing
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
	db.MemCache.data = nil
	db.queue = nil
	err = os.RemoveAll(dbPath)
	log.Info("deleting db")
	return
}

func (db *SimpleDb[T]) Append(itemData T) (id DbItemID, err error) {
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
		Data:     itemData,
	}

	data, err = json.Marshal(item)
	if err != nil {
		return
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
		return 0, err
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
		return &object.Data, nil
	}

	// if object is not in the mem cache, read it from the database file
	log.Debug("seek:", seek)

	db.dataFile.Seek(seek, 0)

	var l []byte = []byte{0, 0} // two bytes
	var n int
	n, err = db.dataFile.Read(l)
	if err != nil {
		return
	}

	log.Debug("bytes read:", n)

	itemLen := binary.LittleEndian.Uint16(l)

	log.Debug("itemLen:", itemLen, ":", l[0], l[1])

	var data []byte = make([]byte, itemLen)
	n, err = db.dataFile.Read(data)
	if err != nil {
		return
	}

	log.Debug("bytes read:", n)
	log.Debug(string(data[:]))

	readData := new(DbItem[T])
	err = unmarshalAny(data, readData)
	log.Debug("json:", readData)
	if err != nil {
		log.Debug("error unmarshaling", err)
		return
	}
	if readData.Id != id {
		panic("got wrong id")
	}
	db.addToMemCache(readData)
	return &readData.Data, err
}

func (db *SimpleDb[T]) Close() (err error) {
	var data []byte

	db.dataFile.Close()

	data, err = json.Marshal(db)
	if err != nil {
		return
	}
	err = os.WriteFile(db.infoFilePath, data, 0644)
	return
}

func (db *SimpleDb[T]) Flush() error {
	var err error
	db.dataFile.Close()
	db.dataFile, err = openDbFile(db.dataFilePath) // reopen
	return err
}

func readDbInfo[T any](file string) (db *SimpleDb[T], err error) {
	var data []byte
	data, err = os.ReadFile(file)
	if err != nil {
		return
	}
	db = &SimpleDb[T]{}
	err = json.Unmarshal(data, db)
	return
}

func (db *SimpleDb[T]) addToMemCache(item *DbItem[T]) {

	if len(db.MemCache.queue) == MemCacheMaxItems {
		db.MemCache.data[db.MemCache.queue[0]] = nil
		db.MemCache.queue = db.queue[1:]
	}
	db.MemCache.data[item.Id] = item
	db.MemCache.queue = append(db.MemCache.queue, item.Id)
}
func (db *SimpleDb[T]) getFromMemCache(id DbItemID) (item *DbItem[T], ok bool) {
	item, ok = db.MemCache.data[id]
	db.MemCache.requests++
	if ok {
		db.MemCache.hits++
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
