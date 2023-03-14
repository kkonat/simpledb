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

type Key []byte                 // keys are bytes, since strings would be a waste
type Hash uint32                // no need for larger hashes for now
type ID uint32                  // this is small database, so let's assume it may hold "only" 4 billion k,v pairs
type BlockOffsets map[ID]uint64 // blocks may be up to 4GB?
type KeyHashes map[Hash][]ID
type DeleteFlags map[ID]struct{}

type SimpleDb[T any] struct {
	filePath   string
	fileHandle *os.File

	cache         *cache[T]
	ItemsCount    uint64
	currentOffset uint64 // as blocks may be up to  4GB long, the file length/index must be at least uint64
	highestId     ID
	deleted       DeleteFlags
	blockOffsets  BlockOffsets
	keyMap        KeyHashes
}

// creates a new database or opens an existing one
func NewDb[T any](filename string, cacheSize uint32) (db *SimpleDb[T], err error) {
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	dbDataFile := filepath.Clean(filename)
	dir, file := filepath.Split(dbDataFile)
	dataFilePath := filepath.Join(dir, dbPath, file+dbExt)

	if _, err = os.Stat(dbPath); err != nil {
		os.Mkdir(dbPath, 0700)
	}

	db = &SimpleDb[T]{}

	db.cache = newCache[T](cacheSize)
	db.keyMap = make(KeyHashes, 0)
	db.deleted = make(DeleteFlags)
	db.filePath = dataFilePath

	if _, err = os.Stat(dataFilePath); err == nil {
		// if db file exists
		if db.fileHandle, err = openFile(dataFilePath); err != nil {
			return nil, fmt.Errorf("error opening database file: %w", err)
		}
		if err = db.read(); err != nil {
			return nil, fmt.Errorf("error rebuilding offsets: %w", err)
		}
	} else {
		// if not, initialize empty db
		db.blockOffsets = make(BlockOffsets)
		db.fileHandle, err = openFile(dataFilePath)
	}

	return
}

// Removes the database file from disk, permanently and irreversibly
func Destroy(dbName string) (err error) {
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	if err = os.Remove(dbName); err != nil {
		return fmt.Errorf("error removing datafile: %w", err)
	}
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

	id = db.genNewId()
	timestamp := uint64(time.Now().Unix())

	block := NewBlock(id, timestamp, key, payload) // and then the payload

	// write whole block  at once
	if bytesWritten, err := db.fileHandle.Write(block.getBytes()); err != nil || uint32(bytesWritten) != block.Length {
		return 0, fmt.Errorf("error writing datafile: %w", err)
	}

	db.blockOffsets[id] = db.currentOffset   // add to offsets map
	db.currentOffset += uint64(block.Length) // update current offset
	db.ItemsCount++                          // update db capacity

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

// Gets one key, value pair from the database for the given Id
// This is an internal function,
// Id is also an internal idenifier which  may change on subsequent item updates
func (db *SimpleDb[T]) getById(id ID) (key []byte, value *T, err error) {
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	if !db.has(id) {
		return nil, nil, &NotFoundError{id: id}
	}

	// get from cache if cached
	if object, ok := db.cache.checkaAndGetItem(id); ok {
		if _, ok := db.deleted[id]; !ok { // if in cache and not deleted
			return object.Key, object.Value, nil
		}
	}

	// check if the item has not been markedad deleted
	if _, ok := db.deleted[id]; ok {
		return nil, nil, &NotFoundError{id: id}
	}

	// or else, read data item from the database file
	seek := db.blockLen(id)

	db.fileHandle.Seek(int64(seek), io.SeekStart) // move to the right position in the file
	var blocksize uint32
	if err = binary.Read(db.fileHandle, binary.LittleEndian, &blocksize); err != nil { // read OffsL bytes
		return nil, nil, err
	}
	buff := make([]byte, blocksize)

	db.fileHandle.Seek(int64(seek), io.SeekStart)
	if _, err = db.fileHandle.Read(buff); err != nil {
		return nil, nil, err
	}
	block := &block{}
	block.setBytes(buff)

	key = block.key
	value = new(T)
	// unmarshall payload
	if err := borsh.Deserialize(&value, block.value); err != nil {
		return nil, nil, fmt.Errorf("error deserializing: %w", err)
	}

	// create db Item for caching
	db.cache.addItem(&cacheItem[T]{
		ID:       ID(block.Id),
		LastUsed: block.Timestamp,
		KeyHash:  block.KeyHash,
		Key:      key,
		Value:    value,
	})

	return
}

// Gets a value for the given key
func (db *SimpleDb[T]) Get(searchedKey []byte) (val *T, err error) {
	var candidateKey []byte
	keyHash := getHash([]byte(searchedKey))

	idCandidates, ok := db.keyMap[keyHash]
	if !ok {
		return nil, &NotFoundError{}
	}

	for _, candidate := range idCandidates {
		candidateKey, val, err = db.getById(candidate) // get actual keys
		if err == nil && keysEqual(candidateKey, searchedKey) {
			return val, nil
		}
	}
	return nil, &NotFoundError{}
}

// Updates the value for the given key
func (db *SimpleDb[T]) Update(keyToUpdate []byte, value *T) (id ID, err error) { // TODO: remove ID from return values, as it's an internal id
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	keyHash := getHash([]byte(keyToUpdate))

	idCandidates, ok := db.keyMap[keyHash]
	if !ok {
		return 0, &NotFoundError{}
	}

	// find and delete old key,value pair
	for _, candidate := range idCandidates {
		candidateKey, _, err := db.getById(candidate)
		if err == nil && keysEqual(keyToUpdate, candidateKey) {
			db.deleteById(candidate, keyHash)
			break
		}
	}

	// add themodified key,value pair as a new db Item
	id, err = db.Append(keyToUpdate, value)
	return id, err
}

// Marks item with a given Id for deletion, internal function, may be used for testing/benchmarking
func (db *SimpleDb[T]) deleteById(id ID, keyHash Hash) error {
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	if !db.has(id) {
		return &NotFoundError{id: id}
	}

	//  TODO: remove this check in future, permit multiple deletes, now convenient for testing
	if _, ok := db.deleted[id]; ok {
		return errors.New("item already deleted") // TODO: fail gracefully here instead of error
	} else {
		db.deleted[id] = struct{}{} // set map to empty value as a flag indicating the item is to be deleted
	}

	// remove the item from cache, if it is there
	if cachedItem, ok := db.cache.checkaAndGetItem(id); ok {
		db.cache.removeItem(cachedItem.ID)
	} //else {
	// TODO: debug this - sometimes, when testing execution falls into this "else" branch,
	// even if the object is in fact in the cache
	// log.Info(fmt.Sprintf("why %d ", db.cache.data[id].ID))
	//}

	// remove the item from keyMap

	// this part is the reason  why keyHash must be passed to this method, anyway it is usually known to the calling function
	// if the item is cached, keyHash could be obtained from the cache, but if it's not it would have to be read from disk,
	// which is what I wanted to avoid

	// keyMap contains lists of item ids, which share the same keyHash value, due to hashing collisions
	// to remove keyHash <-> id assignment, one has to find the right Id in the lice
	idList := db.keyMap[keyHash]
	for i, candidateId := range idList {
		if candidateId == id {
			db.keyMap[keyHash] = append(idList[:i], idList[i+1:]...)
		}
	}

	db.ItemsCount--
	return nil
}

// deletes a db item identified with the provided db key
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
	var mtx sync.Mutex
	mtx.Lock()
	defer mtx.Unlock()

	var bytesWritten uint64

	var tmpFile = filepath.Join(dbPath, "temp.sdb")

	if err = db.fileHandle.Close(); err != nil {
		return fmt.Errorf("closing: %w", err)
	}

	if len(db.deleted) == 0 {
		return nil
	}

	if bytesWritten, err = db.copyOmittingDeleted(tmpFile); err != nil {
		return fmt.Errorf("copyomitting: %w", err)
	}

	// substitute the temp file for the datbase file
	if err := os.Remove(db.filePath); err != nil {
		return fmt.Errorf("remove db file: %w", err)
	}

	if bytesWritten == 0 {
		if err := os.Remove(tmpFile); err != nil {
			return fmt.Errorf("remove tmp file: %w", err)
		}
		return nil
	}

	if err := os.Rename(tmpFile, db.filePath); err != nil {
		return fmt.Errorf("rename tmp to db file: %w", err)
	}

	db.cache.cleanup()

	// invalidate all internal structs
	db.blockOffsets = nil
	db.deleted = nil
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
	defer func() {
		src.Close()
		dest.Close()
	}()
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
		if _, ok := db.deleted[ID(header.Id)]; !ok {
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

	return
}

// generates new object id, now it's sequential, later maybe change to guid or what
func (db *SimpleDb[T]) genNewId() (id ID) {
	id = ID(db.highestId)
	db.highestId++
	return
}

// rebuilds internal database structure: offsets map and key hash map
func (db *SimpleDb[T]) read() (err error) {
	var (
		curpos uint64
		lastId ID
		count  uint64
	)

	db.blockOffsets = make(BlockOffsets)
	var header blockHeader

loop:
	for {
		if _, err = db.fileHandle.Seek(int64(curpos), 0); err != nil {
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
		curpos += uint64(header.Length)                                              // update current position in the file
		if ID(header.Id) > lastId {                                                  // keep track of the last id
			lastId = ID(header.Id)
		}
		count++
	}
	db.currentOffset = curpos // update database parameters
	db.ItemsCount = count
	db.highestId = lastId
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
func (db *SimpleDb[T]) blockLen(id ID) uint64 {
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
func getHash(data []byte) Hash {
	return Hash(crc32.Checksum(data, crc32table))
}

// or for fun, let's try this function
// http://www.azillionmonkeys.com/qed/hash.html
