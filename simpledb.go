package simpledb

import (
	"encoding/binary"
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

	"github.com/near/borsh-go"
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
type KeyHashes map[uint32][]ID
type DeleteFlags map[ID]struct{}

type NotFoundError struct {
	id  ID
	Err error
}

func (r *NotFoundError) Error() string {
	return fmt.Sprintf("item %d not found", r.id)
}

type SimpleDb[T any] struct {
	FilePath   string
	fileHandle *os.File

	Cache[T]

	capacity      uint64
	currentOffset int64
	lastId        ID

	markForDelete DeleteFlags
	blockOffsets  Offsets
	keyMap        KeyHashes
}

func Open[T any](filename string) (db *SimpleDb[T], err error) {

	dbDataFile := filepath.Clean(filename)
	dir, file := filepath.Split(dbDataFile)
	dataFilePath := filepath.Join(dbPath, dir, file+dbExt)

	if _, err = os.Stat(dbPath); err != nil {
		os.Mkdir(dbPath, 0700)
	}

	db = &SimpleDb[T]{}

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
		db.blockOffsets = make(Offsets)
		db.fileHandle, err = openDbFile(dataFilePath)
	}

	return
}

func Close[T any](db *SimpleDb[T], dbName string) (err error) {
	_, file := filepath.Split(db.FilePath)
	if name := strings.Split(file, ".")[0]; name != dbName {
		return errors.New("invalid db name provided")
	}
	db.fileHandle.Close()

	db.Cache.Cleanup()
	if err = os.Remove(db.FilePath); err != nil {
		return fmt.Errorf("error removing datafile: %w", err)
	}
	db = nil
	return
}

// Appends a key, value pair to the database, returns added block id, and error, if any
func (db *SimpleDb[T]) Append(key []byte, value *T) (id ID, err error) {
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	var payload []byte
	if payload, err = borsh.Serialize(value); err != nil {
		return 0, fmt.Errorf("error serializing: %w", err)
	}
	if len(payload) > math.MaxUint32 { // payload must not be too large, ha ha
		panic("payload too large")
	}
	//prepare block header values
	id = db.genNewId()
	timestamp := uint64(time.Now().Unix())
	block := NewBlock(id, timestamp, key, payload) // and then the payload

	// write everything at once (pretending to be atomic)
	if bytesWritten, err := db.fileHandle.Write(block.getBytes()); err != nil || uint32(bytesWritten) != block.Offset {
		return 0, fmt.Errorf("error writing datafile: %w", err)
	}

	db.blockOffsets[id] = db.currentOffset  // add to offsets map
	db.currentOffset += int64(block.Offset) // update current offset
	db.capacity++                           // update db capacity

	// Cache the newly added item
	db.Cache.addItem(&CacheItem[T]{
		ID:       ID(block.ID),
		LastUsed: block.Timestamp,
		Key:      key,
		KeyHash:  block.KeyHash,
		Value:    value,
	})

	return id, nil
}

// Gets one block from the database by Id
func (db *SimpleDb[T]) GetById(id ID) (key []byte, val *T, err error) {
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	// check if an object with the requested id has ever been created
	seek, ok := db.blockOffsets[id] // if it is in offsets map
	if !ok {
		return nil, nil, &NotFoundError{id: id}
	}

	// check if it is cached
	if object, ok := db.Cache.getItem(id); ok {
		if _, ok := db.markForDelete[id]; !ok { // if in cache and not deleted
			return object.Key, object.Value, nil
		}
	}

	// check if it has not been marked for deletion
	if _, ok := db.markForDelete[id]; ok {
		return nil, nil, &NotFoundError{id: id}
	}

	// otherwise, read it from the database file
	db.fileHandle.Seek(seek, io.SeekStart) // move to the right position in the file
	var blocksize uint32
	if err = binary.Read(db.fileHandle, binary.LittleEndian, &blocksize); err != nil { // read OffsL bytes
		return nil, nil, err
	}
	db.fileHandle.Seek(seek, io.SeekStart)

	buff := make([]byte, blocksize)
	if _, err = db.fileHandle.Read(buff); err != nil {
		return nil, nil, err
	}
	block := &block{}
	block.setBytes(buff)

	// unmarshall payload
	val = new(T)
	if err := borsh.Deserialize(&val, block.value); err != nil {
		return nil, nil, fmt.Errorf("error deserializing: %w", err)
	}

	// create db Item for caching
	db.Cache.addItem(&CacheItem[T]{
		ID:       ID(block.ID),
		LastUsed: block.Timestamp,
		KeyHash:  block.KeyHash,
		Key:      block.key,
		Value:    val,
	})

	return block.key, val, err
}

func (db *SimpleDb[T]) Get(aKey []byte) (val *T, err error) {
	keyHash := getHash([]byte(aKey))
	ids, ok := db.keyMap[keyHash]
	if !ok {
		return nil, &NotFoundError{}
	}
	var key []byte
	for _, id := range ids {
		key, val, err = db.GetById(id)
		if err == nil && keysEqual(key, aKey) {
			return val, nil
		}
	}
	return nil, &NotFoundError{}
}

func (db *SimpleDb[T]) Update(key []byte) error {
	// GetByKey
	// delete, i.e. mark for delete - remove from cache
	// create new item and copy modified value, reuse ID ???? - if can delete afterwards
	// add to cache?

	return nil
}

// Marks item with a given Id for deletion
func (db *SimpleDb[T]) DeleteById(id ID) error {
	// check if it ever was created
	if _, ok := db.blockOffsets[id]; !ok {
		return &NotFoundError{id: id}
	}

	// check if not already deleted - TODO: remove this check in future
	if _, ok := db.markForDelete[id]; ok {
		return errors.New("item already deleted") // TODO: fail gracefully here instead of error
	} else {
		db.markForDelete[id] = struct{}{}
	}

	// check if the item is cached, if so delete it from the cache
	if ci, ok := db.Cache.data[id]; ok {
		db.Cache.removeItem(ci.ID)
	} //else { TODO: debug this
	// 	log.Info(fmt.Sprintf("why %d ", db.Cache.data[id].ID))
	// }

	db.capacity--
	return nil
}
func (db *SimpleDb[T]) Delete(aKey []byte) (err error) {
	keyHash := getHash([]byte(aKey))
	ids, ok := db.keyMap[keyHash]
	if !ok {
		return &NotFoundError{}
	}
	var key []byte
	var id ID
	for _, id = range ids {
		key, _, err = db.GetById(id)
		if err == nil && keysEqual(key, aKey) {
			db.DeleteById(id)
		}
	}
	return &NotFoundError{id: id}
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

	db.blockOffsets = make(Offsets)
	var header blockHeader

loop:
	for {
		if _, err = db.fileHandle.Seek(curpos, 0); err != nil {
			return err
		}
		if err = binary.Read(db.fileHandle, binary.LittleEndian, &header); err != nil {
			if errors.Is(err, io.EOF) {
				break loop
			} else {
				return err
			}
		}

		db.blockOffsets[ID(header.ID)] = curpos                                      // updat offsets map
		db.keyMap[header.KeyHash] = append(db.keyMap[header.KeyHash], ID(header.ID)) // update kayhashmap
		curpos += int64(header.Offset)                                               // update current position in the file
		if ID(header.ID) > lastId {                                                  // keep track of the last id
			lastId = ID(header.ID)
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

func keysEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func printBuff(bytes []byte) {
	for _, b := range bytes {
		if b >= ' ' && b <= '~' {
			fmt.Printf("%c", b)
		} else {
			fmt.Printf("%02x ", b)
		}
	}
	fmt.Println("")
}

// calculates hash of a buffer - must be fast and relatively collission-safe
// 32 bits for mostly human-readable key values is obviously an overkill
func getHash(data []byte) uint32 {
	return crc32.Checksum(data, crc32table)
}

// or for fun, let's try this function
// http://www.azillionmonkeys.com/qed/hash.html
