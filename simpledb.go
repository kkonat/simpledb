package simpledb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kkonat/simpledb/hash"

	"github.com/near/borsh-go"
	log "github.com/sirupsen/logrus"
)

const (
	DbPath        = "./db"
	DbExt         = ".sdb"
	writeBuffSize = uint64(16 * 1024)
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type Key []byte                 // keys are bytes, since strings would be a waste
type ID uint32                  // this is small database, so let's assume it may hold "only" 4 billion k,v pairs
type BlockOffsets map[ID]uint64 // blocks may be up to 4GB?
type HashIDs map[hash.Type][]ID
type DeleteFlags map[ID]struct{}

type SimpleDb[T any] struct {
	filePath   string
	fileHandle *os.File

	readCache     *cache[T]
	writeBuff     *writeBuff
	ItemsCount    uint64
	currentOffset uint64 // as blocks may be up to  4GB long, the file length/index must be at least uint64
	maxId         ID
	toBeDeleted   DeleteFlags
	blockOffsets  BlockOffsets
	keyMap        HashIDs
	mtx           sync.RWMutex
}

// creates a new database or opens an existing one
func Open[T any](filename string, cacheSize uint32) (db *SimpleDb[T], err error) {

	if cacheSize < 1 {
		panic("cache size must be non-zero")
	}
	db = &SimpleDb[T]{
		filePath:    getFilepath(filename),
		readCache:   newCache[T](cacheSize),
		keyMap:      make(HashIDs, 0),
		toBeDeleted: make(DeleteFlags),
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if _, err = os.Stat(DbPath); err != nil { // create subdir if does not exist
		os.Mkdir(DbPath, 0700)
	}
	if _, err = os.Stat(db.filePath); err == nil { // if db file exists
		if db.fileHandle, err = openFile(db.filePath); err != nil {
			return nil, &DbGeneralError{err: "open"}
		}

		db.writeBuff = newWriteBuff(db.fileHandle)

		if err = db.loadDb(); err != nil {
			return nil, &DbInternalError{oper: "reading db", err: err}
		}
	} else { // if not, initialize empty db
		db.blockOffsets = make(BlockOffsets)
		db.fileHandle, err = openFile(db.filePath)
		db.writeBuff = newWriteBuff(db.fileHandle)
	}
	return
}

// Closes db and Removes the database file from disk, permanently and irreversibly
func (db *SimpleDb[T]) Destroy() (err error) {

	db.mtx.Lock()
	defer db.mtx.Unlock()
	db.fileHandle.Close()
	if err = os.Remove(db.filePath); err != nil {
		return &DbInternalError{oper: "removing datafile", err: err}
	}
	return
}

// Forcefully deletes database file from disk
func DeleteDbFile(file string) error {
	path := getFilepath(file)
	return os.Remove(path)
}

// Appends a key, value pair to the database, returns added block id, and error, if any
func (db *SimpleDb[T]) Append(key []byte, value *T) (id ID, err error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	return db.appendWOLock(key, value)
}

func (db *SimpleDb[T]) appendWOLock(key []byte, value *T) (id ID, err error) {

	id = db.genNewId()
	keyHash := hash.Get(key)

	// Cache the newly added item in readCache
	cacheItem := &Item[T]{
		ID:      ID(id),
		Key:     key,
		KeyHash: keyHash,
		Value:   value,
	}
	db.readCache.add(cacheItem)

	// Cache the newly added item in writeCache
	srlzdValue, err := borsh.Serialize(value)
	if err != nil {
		panic("todo: handle serialization failure")
	}
	block := NewBlock(id, key, srlzdValue)
	db.writeBuff.append(id, block.getBytes())

	if db.writeBuff.size() > writeBuffSize {
		var blockOffsets []idOffset

		if blockOffsets, err = db.writeBuff.flush(); err != nil {
			return 0, &DbInternalError{oper: "flushing cache"}
		}
		db.updateBlockOffsets(blockOffsets)
	}
	db.keyMap[keyHash] = append(db.keyMap[keyHash], id)
	return id, nil
}

func (db *SimpleDb[T]) updateBlockOffsets(bo []idOffset) {
	for i := 0; i < len(bo); i++ {
		db.blockOffsets[bo[i].id] = db.currentOffset // add to offsets map
		db.currentOffset += bo[i].offset
		db.ItemsCount++ // update db capacity
	}
}

// Gets one key, value pair from the database for the given Id
// This is an internal function,
// Id is also an internal idenifier which  may change on subsequent item updates
func (db *SimpleDb[T]) getById(id ID) (key []byte, value *T, err error) {

	// check if the item has not been markedad deleted
	if _, ok := db.toBeDeleted[id]; ok {
		return nil, nil, &NotFoundError{id: id}
	}

	// maybe it is in the read cache
	if object, ok := db.readCache.checkAndGet(id); ok {
		db.readCache.touch(id) // yes it's in the read cache, so mark as recently accessed
		return object.Key, object.Value, nil
	}

	// it may happen it is  not in the read cache
	if db.writeBuff.has(id) { // but maybe it still in the write buffer and has not been saved yet
		bo, err := db.writeBuff.flush() // if so, then flush the whole buffer
		if err != nil {
			return nil, nil, err
		}
		db.updateBlockOffsets(bo) // update whats in the db after flush
	}

	if !db.has(id) { // so check if it's not in the file anyway
		return nil, nil, &NotFoundError{id: id}
	}

	// if it is, readit  from the database file
	seek := db.blockLen(id)

	db.fileHandle.Seek(int64(seek), io.SeekStart) // move to the right position in the file
	header := blockHeader{}
	fmt.Printf("seek: %v	", seek)
	if err = header.read(db.fileHandle); err != nil { // read OffsL bytes
		fmt.Printf(" ERROR %v	", err)
		return nil, nil, err
	}
	// fmt.Printf("header: %+v\n", header)
	buff := make([]byte, header.Length)

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
		return nil, nil, &DbInternalError{oper: "deserializing", err: err}
	}

	// create db Item for caching
	db.readCache.add(&Item[T]{
		ID:      ID(block.Id),
		KeyHash: block.KeyHash,
		Key:     key,
		Value:   value,
	})
	return
}

// Gets a value for the given key
func (db *SimpleDb[T]) Get(searchedKey []byte) (val *T, err error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	var candidateKey []byte
	keyHash := hash.Get([]byte(searchedKey))

	idCandidates, ok := db.keyMap[keyHash]
	if !ok {
		return nil, &NotFoundError{}
	}
	if len(idCandidates) > 1 {
		fmt.Println("hash Collisions: ", len(idCandidates))
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
func (db *SimpleDb[T]) Update(keyToUpdate []byte, value *T) (err error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	keyHash := hash.Get([]byte(keyToUpdate))

	idCandidates, ok := db.keyMap[keyHash]
	if !ok {
		return &NotFoundError{}
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
	_, err = db.appendWOLock(keyToUpdate, value)

	// Update deletes the old and addsthe new item do db, and to the cache so it's automatically cached, and the freshest in the cache
	return err
}

// Marks item with a given Id for deletion, internal function, may be used for testing/benchmarking
func (db *SimpleDb[T]) deleteById(id ID, keyHash hash.Type) error {
	if db.readCache.check(id) {
		db.readCache.remove(id)
	}
	if db.writeBuff.has(id) {
		db.writeBuff.remove(id)
	}

	if !db.has(id) { // should be in the file then
		return &NotFoundError{id: id}
	}

	// else not yet in the file but in either of the two caches

	// remove the item from caches, if it is there

	// may be still on the disk, so mark for deletion
	db.toBeDeleted[id] = struct{}{} // set map to empty value as a flag indicating the item is to be deleted
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
	db.mtx.Lock()
	defer db.mtx.Unlock()

	keyHash := hash.Get([]byte(aKey))
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
			return nil
		}
	}
	return &NotFoundError{id: id}
}

// closes the database and performs necessary housekeeping
func (db *SimpleDb[T]) Close() (err error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	var bytesWritten uint64

	var tmpFile = filepath.Join(DbPath, "temp.sdb")
	if _, err := db.writeBuff.flush(); err != nil {
		return &DbInternalError{oper: "flushing cche"}
	}

	if err = db.fileHandle.Close(); err != nil {
		return &DbInternalError{oper: "closing: %w", err: err}
	}
	if len(db.toBeDeleted) != 0 {
		if bytesWritten, err = db.copyOmittingDeleted(tmpFile); err != nil {
			return &DbInternalError{oper: "copyomitting: %w", err: err}
		}
		if bytesWritten == 0 {
			if err := os.Remove(tmpFile); err != nil {
				return &DbInternalError{oper: "removing tmp file: %w", err: err}
			}
			return nil
		}

		// substitute the temp file for the datbase file
		if err := os.Remove(db.filePath); err != nil {
			return &DbInternalError{oper: "removing db file: %w", err: err}
		}

		if err := os.Rename(tmpFile, db.filePath); err != nil {
			return &DbInternalError{oper: "renaming tmp to db file: %w", err: err}
		}
	}
	db.readCache.cleanup()

	// invalidate all internal structs
	db.blockOffsets = nil
	db.toBeDeleted = nil
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
		if err = header.read(src); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
				break loop
			} else {
				return 0, err
			}
		}
		if _, ok := db.toBeDeleted[ID(header.Id)]; !ok {
			buff := make([]byte, header.Length)
			if _, err = src.Seek(curpos, 0); err != nil {
				return 0, err
			}
			if _, err = src.Read(buff); err != nil {
				return 0, err
			}
			if n, err := dest.Write(buff); err != nil {
				return 0, err
			} else {
				bytesWritten += uint64(n)

			}
		}
		curpos += int64(header.Length)
	}
	return bytesWritten, err
}

// generates new object id, now it's sequential, later maybe change to guid or what
func (db *SimpleDb[T]) genNewId() (id ID) {
	id = ID(db.maxId)
	db.maxId++
	return
}

// rebuilds internal database structure: offsets map and key hash map
func (db *SimpleDb[T]) loadDb() (err error) {
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
		if err = header.read(db.fileHandle); err != nil {
			if errors.Is(err, io.EOF) {
				break loop
			} else {
				return err
			}
		}

		db.blockOffsets[ID(header.Id)] = curpos // updat offsets map
		db.keyMap[header.KeyHash] = append(     // update kayhashmap
			db.keyMap[header.KeyHash],
			ID(header.Id))
		curpos += uint64(header.Length) // update current position in the file
		if ID(header.Id) > lastId {     // keep track of the last id
			lastId = ID(header.Id)
		}
		count++
	}
	db.currentOffset = curpos // update database parameters
	db.ItemsCount = count
	db.maxId = lastId + 1
	return nil
}

// checks if the database has an element with the given ID
func (db *SimpleDb[T]) has(id ID) (ok bool) {
	_, ok = db.blockOffsets[id]
	return
}

func (db *SimpleDb[T]) blockLen(id ID) uint64 {
	offs, ok := db.blockOffsets[id]
	if !ok {
		// panics, because this func must be called from after the has() check
		panic("item never created")
	}
	fmt.Println("block id:", id, " offs:", offs)
	return offs
}
