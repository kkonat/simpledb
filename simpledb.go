package simpledb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	dbPath = "./db"
	dbExt  = ".sdb"
)

var crc32table *crc32.Table

func init() {
	crc32table = crc32.MakeTable(0x82f63b78)
	log.SetLevel(log.DebugLevel)
}

type ID uint32
type Offsets map[ID]int64
type KeyHashes map[uint32]ID
type DeleteFlags map[ID]struct{}

type SimpleDb[T any] struct {
	FilePath   string
	fileHandle *os.File

	Cache[T]

	capacity      uint64
	currentOffset int64
	lastId        ID

	markForDelete DeleteFlags
	itemOffsets   Offsets
	keyMap        KeyHashes
}

type Item[T any] struct {
	ID       ID
	LastUsed uint64
	KeyHash  uint32
	Key      string
	Value    *T
}

func Connect[T any](filename string) (db *SimpleDb[T], err error) {

	dbDataFile := filepath.Clean(filename)
	dir, file := filepath.Split(dbDataFile)
	dataFilePath := filepath.Join(dbPath, dir, file+dbExt)

	if _, err = os.Stat(dbPath); err != nil {
		os.Mkdir(dbPath, 0700)
	}

	db = &SimpleDb[T]{}
	// initialize cache
	db.Cache.Initialize()
	db.keyMap = make(KeyHashes, 0)
	db.markForDelete = make(DeleteFlags)
	db.FilePath = dataFilePath

	if _, err = os.Stat(dataFilePath); err == nil {
		// if db file exists
		if db.fileHandle, err = openDbFile(dataFilePath); err != nil {
			return nil, fmt.Errorf("error opening database file: %w", err)
		}
		if err = db.rebuildOffsets(); err != nil {
			return nil, fmt.Errorf("error rebuilding offsets: %w", err)
		}
	} else {
		// if not, initialize empty db
		db.itemOffsets = make(Offsets)
		db.fileHandle, err = openDbFile(dataFilePath)
	}

	return
}

func Destroy[T any](db *SimpleDb[T], dbName string) (err error) {
	_, file := filepath.Split(db.FilePath)
	if name := strings.Split(file, ".")[0]; name != dbName {
		return errors.New("invalid db name provided")
	}
	db.fileHandle.Close()

	db.Cache.data = nil
	db.queue = nil
	if err = os.Remove(db.FilePath); err != nil {
		return fmt.Errorf("error removing datafile: %w", err)
	}
	db = nil
	return
}

// Data base item binary data structure
const (
	OffsPos    = 0                     // Offset, this is the offset to the next item in the file, i.e. this item lenght
	OffsL      = 4                     // 4 bytes
	IDPos      = OffsPos + OffsL       // Objec ID
	IDL        = 4                     // 4 bytes
	TimePos    = IDPos + IDL           // Timestamp
	TimeL      = 8                     // 8 bytes
	KeyHashPos = TimePos + TimeL       // Key hash
	KeyHashL   = 4                     // 4 bytes
	KeyLenPos  = KeyHashPos + KeyHashL // Key Length
	KeyLenL    = 2                     // 2 bytes
	KeyPos     = KeyLenPos + KeyLenL   // Key, variable lenght
	//                                    payload of variable leghts, goes after the key
)

// Appends a key, value pair to the database, returns added item id, and error, if any
func (db *SimpleDb[T]) Append(key string, value *T) (id ID, err error) {
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	var payload []byte // prepare payload - TODO: replace json with binary encoder
	if payload, err = json.Marshal(value); err != nil {
		return 0, fmt.Errorf("error marshalling: %w", err)
	}
	if len(payload) > math.MaxUint32 { // payload must not be too large, ha ha
		panic("payload too large")
	}

	//prepare item header values
	id = db.genNewId()
	timestamp := uint64(time.Now().Unix())
	hash := calcHash([]byte(key))

	// put together the header
	header := binary.LittleEndian.AppendUint32([]byte{}, uint32(id))      // unique item id
	header = binary.LittleEndian.AppendUint64(header, timestamp)          // created timestamp
	keyData := binary.LittleEndian.AppendUint32([]byte{}, hash)           // key hash
	keyData = binary.LittleEndian.AppendUint16(keyData, uint16(len(key))) // key length
	keyData = append(keyData, []byte(key)...)                             // key value
	header = append(header, keyData...)                                   //

	offset := len(header) + len(payload) + 4 // comppute dat item size plus 4 for the offset itself

	item := binary.LittleEndian.AppendUint32([]byte{}, uint32(offset)) // put offset in front
	item = append(item, header...)                                     // then goes the header
	item = append(item, payload...)                                    // and then the payload

	// write everything at once (pretending to be atomic)
	if bytesWritten, err := db.fileHandle.Write(item); err != nil || bytesWritten != offset {
		return 0, fmt.Errorf("error writing datafile: %w", err)
	}

	db.itemOffsets[id] = db.currentOffset // add to offsets map
	db.currentOffset += int64(offset)     // update current offset
	db.capacity++                         // update db capacity
	// Cache the newly added item
	db.Cache.addItem(&Item[T]{
		ID:       id,
		LastUsed: timestamp,
		Key:      key,
		KeyHash:  hash,
		Value:    value,
	})

	return id, nil
}

// Gets one Item from the database by Id
func (db *SimpleDb[T]) GetById(id ID) (rd *T, err error) {
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	// check if an object with the requested id has ever been created
	seek, ok := db.itemOffsets[id] // if it is in offsets map
	if !ok {
		return nil, errors.New("item not found in the database")
	}

	// check if it is cached
	if object, ok := db.Cache.getItem(id); ok {
		if _, ok := db.markForDelete[id]; !ok { // if in cache and not deleted
			return object.Value, nil
		}
	}

	// check if it has not been marked for deletion
	if _, ok := db.markForDelete[id]; ok {
		return nil, errors.New("item not found in the database")
	}

	// otherwise, read it from the database file
	db.fileHandle.Seek(seek, io.SeekStart)             // move to the right position in the file
	buff := make([]byte, OffsL)                        // first read only the the item length, i.e. the offset
	if _, err = db.fileHandle.Read(buff); err != nil { // read OffsL bytes
		return nil, err
	}
	offset := binary.LittleEndian.Uint32(buff)
	buff = make([]byte, offset)            // resize the buffer to hold the whole item
	db.fileHandle.Seek(seek, io.SeekStart) // re-read the whole item (to make slice constants meaningful)
	if _, err = db.fileHandle.Read(buff); err != nil {
		return nil, err
	}
	// decode data
	itemId := binary.LittleEndian.Uint16(buff[IDPos : IDPos+IDL])
	timestamp := binary.LittleEndian.Uint64(buff[TimePos : TimePos+TimeL])
	keyHash := binary.LittleEndian.Uint32(buff[KeyHashPos : KeyHashPos+KeyHashL])
	keylen := binary.LittleEndian.Uint16(buff[KeyLenPos : KeyLenPos+KeyLenL])
	key := string(buff[KeyPos : KeyPos+keylen])

	// unmarshall payload
	newData := new(T)
	if err := json.Unmarshal(buff[KeyPos+keylen:], newData); err != nil {
		return nil, fmt.Errorf("error unmarshalling: %w", err)
	}

	// create db Item for caching
	db.Cache.addItem(&Item[T]{
		ID:       ID(itemId),
		LastUsed: timestamp,
		KeyHash:  keyHash,
		Key:      key,
		Value:    newData,
	})

	return newData, err
}

// Marks item with a given Id for deletion
func (db *SimpleDb[T]) DeleteById(id ID) error {
	// check if it ever was created
	if _, ok := db.itemOffsets[id]; !ok {
		return errors.New("item not found in the database")
	}

	// check if not already deleted - TODO: remove this check in future
	if _, ok := db.markForDelete[id]; ok {
		return errors.New("item already deleted") // TODO: fail gracefully here instead of error
	} else {
		db.markForDelete[id] = struct{}{}
	}

	// check if the itm is cached, if so delete it from the cache
	if _, ok := db.Cache.data[id]; ok {
		db.Cache.removeItem(id)
	}

	db.capacity--
	return nil
}

// closes the database and performs necessary housekeeping
func (db *SimpleDb[T]) Close() (err error) {

	return db.fileHandle.Close()
	// TODO
	// db.toBeDeleted is a map of all objects marked to be toBeDeleted
	// need to copy the database, while skipping this object
}

// generates new object id, now it's sequential, lager maybe change to guid or what
func (db *SimpleDb[T]) genNewId() (id ID) {
	id = ID(db.lastId)
	db.lastId++
	return
}

// rebuilds internal database structure: offsets map and key hash map
func (db *SimpleDb[T]) rebuildOffsets() (err error) {
	var (
		curpos int64
		lastId ID
		count  uint64
	)

	db.itemOffsets = make(Offsets)

	buff := make([]byte, OffsL+IDL+TimeL+KeyHashL+KeyLenL) // only this data is needed, fixed length
loop:
	for {
		if _, err = db.fileHandle.Seek(curpos, 0); err != nil {
			return err
		}
		if _, err = db.fileHandle.Read(buff); err != nil {
			if errors.Is(err, io.EOF) {
				break loop
			} else {
				return err
			}
		}
		skip := binary.LittleEndian.Uint32(buff[OffsPos : OffsPos+OffsL])
		id := ID(binary.LittleEndian.Uint32(buff[IDPos : IDPos+IDL]))
		hash := binary.LittleEndian.Uint32(buff[KeyHashPos : KeyHashPos+KeyHashL])
		keyLen := binary.LittleEndian.Uint16(buff[KeyLenPos : KeyLenPos+KeyLenL])

		// extra sanity check, to see whether the db file is not corrupted
		kbuff := make([]byte, keyLen)
		if _, err = db.fileHandle.Read(kbuff); err != nil {
			return err
		}
		readKeyHash := calcHash(kbuff)
		if readKeyHash != hash { // key hash saved in db must be the same as the hash recomputed from key
			panic("corrupted database file")
		}

		db.itemOffsets[ID(id)] = curpos // updat offsets map
		db.keyMap[hash] = ID(id)        // update kayhashmap
		curpos += int64(skip)           // update current position in the file
		if id > lastId {                // keep track of the last id
			lastId = id
		}
		count++
	}
	db.currentOffset = curpos // update database parameters
	db.capacity = count
	db.lastId = lastId
	return nil
}

func openDbFile(path string) (file *os.File, err error) {
	file, err = os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	return
}

// calculates hash of a buffer - must be fast and relatively collission-safe
// 32 bits for mostly human-readable key values is obviously an overkill
func calcHash(data []byte) uint32 {
	return crc32.Checksum(data, crc32table)
}

// or for fun, let's try this function
// http://www.azillionmonkeys.com/qed/hash.html
