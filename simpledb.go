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
type offsets map[ID]int64
type keyHashes map[uint32][]ID
type deleteFlags map[ID]struct{}

type NotFoundError struct {
	id  ID
	Err error
}

func (r *NotFoundError) Error() string {
	return fmt.Sprintf("item %d not found", r.id)
}

type SimpleDb[T any] struct {
	filePath   string
	fileHandle *os.File

	cache[T]

	capacity      uint64
	currentOffset int64
	lastId        ID

	markedForDelete deleteFlags
	blockOffsets    offsets
	keyMap          keyHashes
}

func Open[T any](filename string) (db *SimpleDb[T], err error) {

	dbDataFile := filepath.Clean(filename)
	dir, file := filepath.Split(dbDataFile)
	dataFilePath := filepath.Join(dir, dbPath, file+dbExt)

	if _, err = os.Stat(dbPath); err != nil {
		os.Mkdir(dbPath, 0700)
	}

	db = &SimpleDb[T]{}

	db.cache.initialize()
	db.keyMap = make(keyHashes, 0)
	db.markedForDelete = make(deleteFlags)
	db.filePath = dataFilePath

	if _, err = os.Stat(dataFilePath); err == nil {
		// if db file exists
		if db.fileHandle, err = openFile(dataFilePath); err != nil {
			return nil, fmt.Errorf("error opening database file: %w", err)
		}
		if err = db.rebuildOffsets(); err != nil {
			return nil, fmt.Errorf("error rebuilding offsets: %w", err)
		}
	} else {
		// if not, initialize empty db
		db.blockOffsets = make(offsets)
		db.fileHandle, err = openFile(dataFilePath)
	}

	return
}

// Closes and removes the database file from disk, requires db filename to be provided, for safety
func Destroy[T any](db *SimpleDb[T], dbName string) (err error) {
	_, file := filepath.Split(db.filePath)
	if name := strings.Split(file, ".")[0]; name != dbName {
		return errors.New("invalid db name provided")
	}
	db.fileHandle.Close()
	db.cache.cleanup()
	if err = os.Remove(db.filePath); err != nil {
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
	if bytesWritten, err := db.fileHandle.Write(block.getBytes()); err != nil || uint32(bytesWritten) != block.Length {
		return 0, fmt.Errorf("error writing datafile: %w", err)
	}

	db.blockOffsets[id] = db.currentOffset  // add to offsets map
	db.currentOffset += int64(block.Length) // update current offset
	db.capacity++                           // update db capacity

	// Cache the newly added item
	db.cache.addItem(&cacheItem[T]{
		ID:       ID(block.Id),
		LastUsed: block.Timestamp,
		Key:      key,
		KeyHash:  block.KeyHash,
		Value:    value,
	})

	return id, nil
}

// Gets one block from the database by Id, internal function, Id is internal idenifier and may change on subsequent item updates
func (db *SimpleDb[T]) getById(id ID) (key []byte, val *T, err error) {
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	if !db.has(id) {
		return nil, nil, &NotFoundError{id: id}
	}

	// check if it is cached
	if object, ok := db.cache.getItem(id); ok {
		if _, ok := db.markedForDelete[id]; !ok { // if in cache and not deleted
			return object.Key, object.Value, nil
		}
	}

	// check if it has not been marked for deletion
	if _, ok := db.markedForDelete[id]; ok {
		return nil, nil, &NotFoundError{id: id}
	}

	// otherwise, read it from the database file
	seek := db.blockLen(id)                // if it is in offsets map
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
	db.cache.addItem(&cacheItem[T]{
		ID:       ID(block.Id),
		LastUsed: block.Timestamp,
		KeyHash:  block.KeyHash,
		Key:      block.key,
		Value:    val,
	})

	return block.key, val, err
}

// Gets a value for the given key
func (db *SimpleDb[T]) Get(aKey []byte) (val *T, err error) {
	keyHash := getHash([]byte(aKey))
	ids, ok := db.keyMap[keyHash]
	if !ok {
		return nil, &NotFoundError{}
	}
	var key []byte
	for _, id := range ids {
		key, val, err = db.getById(id)
		if err == nil && keysEqual(key, aKey) {
			return val, nil
		}
	}
	return nil, &NotFoundError{}
}

// Updates the value for the given key
func (db *SimpleDb[T]) Update(aKey []byte, value *T) (id ID, err error) { // TODO: remove ID from return values, as it's an internal id
	keyHash := getHash([]byte(aKey))
	ids, ok := db.keyMap[keyHash]
	if !ok {
		return 0, &NotFoundError{}
	}
	for _, oldId := range ids {
		key, _, err := db.getById(oldId)
		if err == nil && keysEqual(aKey, key) {
			db.deleteById(oldId, keyHash)
			break
		}
	}
	id, err = db.Append(aKey, value)
	return id, err
}

// Marks item with a given Id for deletion, internal function, may be used for testing/benchmarking
func (db *SimpleDb[T]) deleteById(id ID, keyHash uint32) error {

	if !db.has(id) {
		return &NotFoundError{id: id}
	}

	//  TODO: remove this check in future, permit multiple deletes, now convenient for testing
	if _, ok := db.markedForDelete[id]; ok {
		return errors.New("item already deleted") // TODO: fail gracefully here instead of error
	} else {
		db.markedForDelete[id] = struct{}{}
	}

	if ci, ok := db.cache.getItem(id); ok {
		db.cache.removeItem(ci.ID)
	} //else { TODO: debug this
	// 	log.Info(fmt.Sprintf("why %d ", db.cache.data[id].ID))
	// }
	ids := db.keyMap[keyHash]
	for i, cid := range ids {
		if cid == id {
			db.keyMap[keyHash] = append(ids[:i], ids[i+1:]...)
		}
	}
	db.capacity--
	return nil
}

// deletes a db entry for the given db key
func (db *SimpleDb[T]) Delete(aKey []byte) (err error) {
	keyHash := getHash([]byte(aKey))
	ids, ok := db.keyMap[keyHash]
	if !ok {
		return &NotFoundError{}
	}
	var key []byte
	var id ID
	for _, id = range ids {
		key, _, err = db.getById(id)
		if err == nil && keysEqual(key, aKey) {
			db.deleteById(id, keyHash)
		}
	}
	return &NotFoundError{id: id}
}

// closes the database and performs necessary housekeeping
func (db *SimpleDb[T]) Close() (err error) {
	const tmpFile = dbPath + "temp.sdb"
	var mtx sync.Mutex
	var bytesWritten uint64

	mtx.Lock()
	defer mtx.Unlock()

	db.fileHandle.Close()
	if len(db.markedForDelete) == 0 {
		return nil
	}

	if bytesWritten, err = db.copyOmittingDeleted(tmpFile); err != nil {
		return err
	}

	// substitute the temp file for the datbase file
	if err := os.Remove(db.filePath); err != nil {
		return err
	}

	if bytesWritten == 0 {
		if err := os.Remove(tmpFile); err != nil {
			return err
		}
		return nil
	}
	if err := os.Rename(tmpFile, db.filePath); err != nil {
		return err
	}

	db.cache.cleanup()

	// invalidate all internal structs
	db.blockOffsets = nil
	db.markedForDelete = nil
	for k := range db.keyMap {
		delete(db.keyMap, k)
	}
	db.keyMap = nil

	return
}

func (db *SimpleDb[T]) copyOmittingDeleted(tmpFile string) (bytesWritten uint64, err error) {
	var (
		curpos int64
		header blockHeader
	)
	// copy the database file to a temp file, while omitting deleted items
	dest, err := openFile(tmpFile)
	if err != nil {
		return 0, err
	}
	src, err := openFile(db.filePath)
	if err != nil {
		return 0, err
	}
loop:
	for {
		if _, err = src.Seek(curpos, 0); err != nil {
			return 0, err
		}
		if err = binary.Read(src, binary.LittleEndian, &header); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
				break loop
			} else {
				return 0, err
			}
		}
		if _, ok := db.markedForDelete[ID(header.Id)]; !ok {
			buff := make([]byte, header.Length)
			if _, err = src.Seek(curpos, 0); err != nil {
				return 0, err
			}
			if _, err = src.Read(buff); err != nil {
				return 0, err
			}
			if n, err := dest.Write(buff); err != nil {
				bytesWritten += uint64(n)
				return bytesWritten, err
			}
		}
		curpos += int64(header.Length)
	}
	src.Close()
	dest.Close()

	return
}

// generates new object id, now it's sequential, later maybe change to guid or what
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

	db.blockOffsets = make(offsets)
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

		db.blockOffsets[ID(header.Id)] = curpos                                      // updat offsets map
		db.keyMap[header.KeyHash] = append(db.keyMap[header.KeyHash], ID(header.Id)) // update kayhashmap
		curpos += int64(header.Length)                                               // update current position in the file
		if ID(header.Id) > lastId {                                                  // keep track of the last id
			lastId = ID(header.Id)
		}
		count++
	}
	db.currentOffset = curpos // update database parameters
	db.capacity = count
	db.lastId = lastId
	return nil
}

// helper functions

// opens the file, used for keeping track of the open/rw/create mode
func openFile(path string) (file *os.File, err error) {
	file, err = os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	return
}

// checks if the database has an element with the given ID
func (db *SimpleDb[T]) has(id ID) (ok bool) {
	_, ok = db.blockOffsets[id]
	return
}

// returns lenght of a block of an item with the given ID in the database, reads this data from RAM
func (db *SimpleDb[T]) blockLen(id ID) int64 {
	offs, ok := db.blockOffsets[id]
	if !ok {
		// panics, because this func must be called from after the has() check
		panic("item never created")
	}
	return offs
}

// compares two keys provided as []byte slices
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

// prints out the []byte slice content
func printBytes(bytes []byte) {
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
