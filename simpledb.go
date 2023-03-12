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

// Database block binary data structure
const (
	OffsPos    = 0                     // Offset, this is the offset to the next block in the file, i.e. this block lenght
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
	hash := getHash([]byte(key))

	// put together the header
	header := binary.LittleEndian.AppendUint32([]byte{}, uint32(id))      // unique block id
	header = binary.LittleEndian.AppendUint64(header, timestamp)          // created timestamp
	keyData := binary.LittleEndian.AppendUint32([]byte{}, hash)           // key hash
	keyData = binary.LittleEndian.AppendUint16(keyData, uint16(len(key))) // key length
	keyData = append(keyData, []byte(key)...)                             // key value
	header = append(header, keyData...)                                   //

	offset := len(header) + len(payload) + 4 // comppute dat block size plus 4 for the offset itself

	block := binary.LittleEndian.AppendUint32([]byte{}, uint32(offset)) // put offset in front
	block = append(block, header...)                                    // then goes the header
	block = append(block, payload...)                                   // and then the payload

	// write everything at once (pretending to be atomic)
	if bytesWritten, err := db.fileHandle.Write(block); err != nil || bytesWritten != offset {
		return 0, fmt.Errorf("error writing datafile: %w", err)
	}

	db.blockOffsets[id] = db.currentOffset // add to offsets map
	db.currentOffset += int64(offset)      // update current offset
	db.capacity++                          // update db capacity
	// Cache the newly added item
	db.Cache.addItem(&CacheItem[T]{
		ID:       id,
		LastUsed: timestamp,
		Key:      key,
		KeyHash:  hash,
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
		return nil, nil, errors.New("item not found in the database")
	}

	// check if it is cached
	if object, ok := db.Cache.getItem(id); ok {
		if _, ok := db.markForDelete[id]; !ok { // if in cache and not deleted
			return object.Key, object.Value, nil
		}
	}

	// check if it has not been marked for deletion
	if _, ok := db.markForDelete[id]; ok {
		return nil, nil, errors.New("item not found in the database")
	}

	// otherwise, read it from the database file
	db.fileHandle.Seek(seek, io.SeekStart)             // move to the right position in the file
	buff := make([]byte, OffsL)                        // first read only the the item length, i.e. the offset
	if _, err = db.fileHandle.Read(buff); err != nil { // read OffsL bytes
		return nil, nil, err
	}
	offset := binary.LittleEndian.Uint32(buff)
	buff = make([]byte, offset)            // resize the buffer to hold the whole block
	db.fileHandle.Seek(seek, io.SeekStart) // re-read the whole block (to make slice constants meaningful)
	if _, err = db.fileHandle.Read(buff); err != nil {
		return nil, nil, err
	}
	// decode data
	itemId := binary.LittleEndian.Uint16(buff[IDPos : IDPos+IDL])
	timestamp := binary.LittleEndian.Uint64(buff[TimePos : TimePos+TimeL])
	keyHash := binary.LittleEndian.Uint32(buff[KeyHashPos : KeyHashPos+KeyHashL])
	keylen := binary.LittleEndian.Uint16(buff[KeyLenPos : KeyLenPos+KeyLenL])
	key = buff[KeyPos : KeyPos+keylen]

	// unmarshall payload
	val = new(T)
	if err := borsh.Deserialize(&val, buff[KeyPos+keylen:]); err != nil {
		return nil, nil, fmt.Errorf("error deserializing: %w", err)
	}

	// create db Item for caching
	db.Cache.addItem(&CacheItem[T]{
		ID:       ID(itemId),
		LastUsed: timestamp,
		KeyHash:  keyHash,
		Key:      key,
		Value:    val,
	})

	return key, val, err
}

func (db *SimpleDb[T]) GetByKey(aKey []byte) (val *T, err error) {
	keyHash := getHash([]byte(aKey))
	ids, ok := db.keyMap[keyHash]
	if !ok {
		return nil, errors.New("key not found")
	}
	var key []byte
	for _, id := range ids {
		key, val, err = db.GetById(id)
		if err == nil && keysEqual(key, aKey) {
			return val, nil
		}
	}
	return nil, errors.New("key not found")
}

// Marks item with a given Id for deletion
func (db *SimpleDb[T]) DeleteById(id ID) error {
	// check if it ever was created
	if _, ok := db.blockOffsets[id]; !ok {
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

	db.blockOffsets = make(Offsets)

	buff := make([]byte, OffsL+IDL+TimeL+KeyHashL /*+KeyLenL*/) // only this data is needed, fixed length
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
		// keyLen := binary.LittleEndian.Uint16(buff[KeyLenPos : KeyLenPos+KeyLenL])

		db.blockOffsets[ID(id)] = curpos                  // updat offsets map
		db.keyMap[hash] = append(db.keyMap[hash], ID(id)) // update kayhashmap
		curpos += int64(skip)                             // update current position in the file
		if id > lastId {                                  // keep track of the last id
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
