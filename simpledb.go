package simpledb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	dbPath        = "./db"
	dbExt         = ".sdb"
	CacheMaxItems = 100
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type DbItemID int32
type OffsetsData map[DbItemID]int64
type Cache[T any] struct {
	data     map[DbItemID]*DbItem[T]
	queue    []DbItemID
	requests uint64
	hits     uint64
}

func (m Cache[T]) getHitRate() float64 {
	if m.requests > 0 {
		return float64(m.hits) / float64(m.requests) * 100
	} else {
		return 0
	}
}

type SimpleDb[T any] struct {
	dataFilePath string
	dataFile     *os.File

	Cache[T]
	deleteFlag map[DbItemID]struct{}
	itemsNo    DbItemID
	currOffset int64
	offsets    OffsetsData
}

type DbItem[T any] struct {
	Id       DbItemID
	Data     T
	LastUsed int64
}

func Connect[T any](filename string) (db *SimpleDb[T], err error) {

	dbDataFile := filepath.Clean(filename)
	dir, file := filepath.Split(dbDataFile)
	dataFilePath := filepath.Join(dbPath, dir, file+dbExt)

	if _, err = os.Stat(dbPath); err != nil {
		os.Mkdir(dbPath, 0700)
	}

	db = &SimpleDb[T]{}

	if _, err = os.Stat(dataFilePath); err == nil {
		// if db file exists
		if db.dataFile, err = openDbFile(dataFilePath); err != nil {
			return nil, fmt.Errorf("error opening database file: %w", err)
		}
		if err = db.rebuildOffsets(); err != nil {
			return nil, fmt.Errorf("error rebuilding offsets: %w", err)
		}
	} else {
		// if not, initialize empty db
		db.offsets = make(OffsetsData)
		db.dataFile, err = openDbFile(dataFilePath)
	}

	// initialize cache
	db.Cache.data = make(map[DbItemID]*DbItem[T])
	db.Cache.queue = make([]DbItemID, 0)

	db.deleteFlag = make(map[DbItemID]struct{})

	db.dataFilePath = dataFilePath

	return
}

func Destroy[T any](db *SimpleDb[T], dbName string) (err error) {
	_, file := filepath.Split(db.dataFilePath)
	if name := strings.Split(file, ".")[0]; name != dbName {
		return errors.New("invalid db name provided")
	}
	db.dataFile.Close()

	db.Cache.data = nil
	db.queue = nil
	if err = os.Remove(db.dataFilePath); err != nil {
		return fmt.Errorf("error removing datafile: %w", err)
	}
	db = nil
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

	// keep this item in cache
	db.offsets[item.Id] = db.currOffset
	db.currOffset += int64(bytesWritten)
	db.addToCache(&item)

	return item.Id, nil
}
func (db *SimpleDb[T]) Delete(id DbItemID) error {
	// check if it ever was created
	if _, ok := db.offsets[id]; !ok {
		return errors.New("item not found in the database")
	}

	// check if not already deleted
	if _, ok := db.deleteFlag[id]; ok {
		return errors.New("item already deleted") // TODO: fail gracefully here instead of error
	} else {
		db.deleteFlag[id] = struct{}{}
	}

	// check if the itm is cached, if so delete it from there to free the cache up
	if _, ok := db.Cache.data[id]; ok {
		// delete from cache
		delete(db.Cache.data, id)
		//find in queue
		for i := 0; i < len(db.Cache.queue); i++ {
			if db.Cache.queue[i] == id { // delete from queue
				db.Cache.queue = append(db.Cache.queue[:i], db.Cache.queue[i+1:]...)
				break
			}
		}
	}

	return nil
}

func (db *SimpleDb[T]) Get(id DbItemID) (rd *T, err error) {
	// check if that object has ever been created
	seek, ok := db.offsets[id]
	if !ok {
		return nil, errors.New("item not found in the database")
	}

	if object, ok := db.getFromCache(id); ok {
		if _, ok := db.deleteFlag[id]; !ok { // if in cache and not deleted
			return &(object.Data), nil
		}
	}
	// if requested to read from disk an object, which has been marked as to be deleted
	if _, ok := db.deleteFlag[id]; ok {
		return nil, errors.New("item not found in the database")
	}

	// otherwise, if object is not in the cache, read it from the database file
	db.dataFile.Seek(seek, 0)

	var itemLen uint16
	if itemLen, err = readInt16(db.dataFile); err != nil {
		return nil, fmt.Errorf("error reading datafile: %w", err)
	}

	var n int
	var data []byte = make([]byte, itemLen)
	if n, err = db.dataFile.Read(data); err != nil || n != int(itemLen) {
		return nil, fmt.Errorf("error reading datafile: %w", err)
	}

	newData := new(DbItem[T])
	if err := json.Unmarshal(data, newData); err != nil {
		return nil, fmt.Errorf("error unmarshalling: %w", err)
	}

	// store ub cache
	db.addToCache(newData)
	return &newData.Data, err
}

func (db *SimpleDb[T]) Close() (err error) {

	return db.dataFile.Close()
	// TODO
	// db.toBeDeleted is a map of all objects marked to be toBeDeleted
	// need to copy the database, while skipping this object
}

func (db *SimpleDb[T]) addToCache(item *DbItem[T]) {
	if len(db.Cache.queue) == CacheMaxItems {
		delete(db.Cache.data, db.Cache.queue[0])
		db.Cache.queue = db.queue[1:]
	}
	db.Cache.data[item.Id] = item
	db.Cache.queue = append(db.Cache.queue, item.Id)
}

func (db *SimpleDb[T]) getFromCache(id DbItemID) (item *DbItem[T], ok bool) {
	db.Cache.requests++
	if item, ok = db.Cache.data[id]; ok {
		db.Cache.hits++
	}
	return
}

func (db *SimpleDb[T]) genNewId() (id DbItemID) {
	id = db.itemsNo
	db.itemsNo++
	return
}

func (db *SimpleDb[T]) rebuildOffsets() (err error) {
	var (
		curpos int64
		skip   uint16
		count  int
	)
	// TODO: add some sanity check, to see whether the file is not corrupted
	db.offsets = make(OffsetsData)
loop:
	for {
		_, err = db.dataFile.Seek(curpos, 0) // go to file start
		if err != nil {
			return err
		}
		skip, err = readInt16(db.dataFile)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break loop
			} else {
				return err
			}
		}
		db.offsets[DbItemID(count)] = curpos
		curpos += int64(skip) + 2
		count++
	}
	db.currOffset = curpos
	db.itemsNo = DbItemID(count)
	return nil
}

func openDbFile(path string) (file *os.File, err error) {
	file, err = os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	return
}
func readInt16(file *os.File) (val uint16, err error) {
	var l []byte = []byte{0, 0} // two bytes
	var n int
	if n, err = file.Read(l); err != nil || n != 2 {
		return 0, fmt.Errorf("error reading datafile: %w", err)
	}
	return binary.LittleEndian.Uint16(l), nil
}
