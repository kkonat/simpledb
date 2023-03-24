package simpledb

import (
	"errors"
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
	bulkWriteSize = int64(16 * 1024)
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type Flag struct{}
type ID uint32 // this is small database, so let's assume it may hold "only" 4 billion k,v pairs

type SimpleDb[T any] struct {
	filePath string
	file     *os.File

	mtx sync.RWMutex

	readCache *cache[T]
	writeBuff *writeBuff

	ItemsCount    int   // number of items in the db
	currentOffset int64 // as blocks may be up to  4GB long, the file length/index must be at least uint64
	maxId         ID    // maximum ID value, used for Item ID generation

	toBeDeleted  map[ID]Flag        // items marked for deletion
	blockOffsets map[ID]int64       // items' ofssets in the file
	keyHashItems map[hash.Type][]ID // to quickly find IDs of items with the given key hash
}

// creates a new database or opens an existing one
func Open[T any](filename string, cacheSize uint32) (db *SimpleDb[T], err error) {

	if cacheSize < 1 {
		panic("cache size must be non-zero")
	}

	db = &SimpleDb[T]{
		filePath:     getFilepath(filename),
		readCache:    newCache[T](cacheSize),
		keyHashItems: make(map[hash.Type][]ID),
		toBeDeleted:  make(map[ID]Flag),
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if _, err = os.Stat(DbPath); err != nil { // create subdir if does not exist
		os.Mkdir(DbPath, 0700)
	}
	if _, err = os.Stat(db.filePath); err == nil { // if db file exists
		if db.file, err = openFile(db.filePath); err != nil {
			return nil, &DbGeneralError{err: "open"}
		}

		db.writeBuff = newWriteBuff()

		if err = db.loadDb(); err != nil {
			return nil, &DbInternalError{oper: "reading db", err: err}
		}
	} else { // if not, initialize empty db
		db.blockOffsets = make(map[ID]int64)
		db.file, err = openFile(db.filePath)
		db.writeBuff = newWriteBuff()
	}
	return
}

// Closes db and Removes the database file from disk, permanently and irreversibly
func (db *SimpleDb[T]) Destroy() (err error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	db.file.Close()
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
func (db *SimpleDb[T]) Append(key string, value *T) (id ID, err error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	return db.appendItem(key, value)
}

func (db *SimpleDb[T]) appendItem(key string, value *T) (id ID, err error) {

	id = db.genNewId()
	keyHash := hash.Get(key)

	// Cache the newly added item in readCache
	cacheItem := &cacheItem[T]{
		id:      ID(id),
		key:     key,
		keyHash: keyHash,
		value:   value,
	}
	db.readCache.add(cacheItem)

	// Cache the newly added item in writeCache
	srlzdValue, err := borsh.Serialize(value)
	if err != nil {
		panic("todo: handle serialization failure")
	}
	block := NewBlock(id, key, srlzdValue)

	// w, err := db.file.Write(block.getBytes())
	// if err != nil {
	// 	return 0, err
	// }

	// db.blockOffsets[id] = db.currentOffset
	// db.currentOffset += int64(w)
	// db.ItemsCount++

	db.writeBuff.append(id, block.getBytes())
	if db.writeBuff.size() > bulkWriteSize {
		if bo, err := db.writeBuff.flush(db.file); err != nil {
			return 0, &DbInternalError{oper: "flushing cache"}
		} else {
			db.updateBlockOffsets(bo)
		}
	}
	if db.keyHashItems[keyHash] == nil {
		db.keyHashItems[keyHash] = make([]ID, 16)
	}
	db.keyHashItems[keyHash] = append(db.keyHashItems[keyHash], id)
	return id, nil
}

func (db *SimpleDb[T]) updateBlockOffsets(bo []blockOffset) {
	for i := 0; i < len(bo); i++ {
		db.blockOffsets[bo[i].id] = db.currentOffset // add to offsets map
		db.currentOffset += bo[i].offset
		db.ItemsCount++ // update db capacity
	}
}

// Gets one key, value pair from the database for the given Id
// This is an internal function,
// Id is also an internal idenifier which  may change on subsequent item updates
func (db *SimpleDb[T]) getItem(id ID) (key string, value *T, err error) {

	if _, ok := db.toBeDeleted[id]; ok {
		return "", nil, &NotFoundError{id: id}
	}

	if object, exists := db.readCache.getIfExists(id); exists {
		db.readCache.touch(id) // if it's in the read cache, mark it as recently accessed
		return object.key, object.value, nil
	}

	if db.writeBuff.contains(id) { // it may be  still in the write buffer which has not been flushed yet
		bo, err := db.writeBuff.flush(db.file)
		if err != nil {
			return "", nil, err
		}
		db.updateBlockOffsets(bo)
	}

	if !db.contains(id) { // re-check if it is now in the file
		return "", nil, &NotFoundError{id: id}
	}
	// if it is, read it from the  file
	offset := db.blockOffsets[id] // never panics, because db.writeBuff.contains() returned true

	// read item from the file
	db.file.Seek(int64(offset), io.SeekStart) // seek to the item  position in the file
	header := blockHeader{}

	if err = header.read(db.file); err != nil { // read OffsL bytes
		return "", nil, err
	}
	buff := make([]byte, header.Length)

	db.file.Seek(int64(offset), io.SeekStart)
	if _, err = db.file.Read(buff); err != nil {
		return "", nil, err
	}
	block := &block{}
	block.setBytes(buff)

	key = block.key
	value = new(T)
	// unmarshall payload
	if err := borsh.Deserialize(&value, block.value); err != nil {
		return "", nil, &DbInternalError{oper: "deserializing", err: err}
	}

	// create db Item for caching
	db.readCache.add(&cacheItem[T]{
		id:      ID(block.Id),
		keyHash: block.KeyHash,
		key:     key,
		value:   value,
	})
	return
}

// Gets a value for the given key
func (db *SimpleDb[T]) Get(key string) (val *T, err error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	var candidateKey string
	keyHash := hash.Get(key)

	idCandidates, ok := db.keyHashItems[keyHash]
	if !ok {
		return nil, &NotFoundError{}
	}

	for _, candidate := range idCandidates {
		candidateKey, val, err = db.getItem(candidate) // get actual keys
		if err == nil && candidateKey == key {
			return val, nil
		}
	}
	return nil, &NotFoundError{}
}

// Updates the value for the given key
func (db *SimpleDb[T]) Update(key string, value *T) (id ID, err error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	keyHash := hash.Get(key)

	idCandidates, ok := db.keyHashItems[keyHash]
	if !ok {
		return 0, &NotFoundError{}
	}

	// find and delete old key,value pair
	for _, candidate := range idCandidates {
		candidateKey, _, err := db.getItem(candidate)
		if err == nil && key == candidateKey {
			db.deleteById(candidate, keyHash)
			break
		}
	}

	// add themodified key,value pair as a new db Item
	id, err = db.appendItem(key, value)

	// Update deletes the old and addsthe new item do db, and to the cache so it's automatically cached, and the freshest in the cache
	return id, err
}

// Marks item with a given Id for deletion, internal function, may be used for testing/benchmarking
func (db *SimpleDb[T]) deleteById(id ID, keyHash hash.Type) error {
	if db.readCache.contains(id) {
		db.readCache.remove(id)
	}
	if db.writeBuff.contains(id) {
		db.writeBuff.remove(id)
	}

	if !db.contains(id) { // should be in the file then
		db.writeBuff.contains(id)
		return &NotFoundError{id: id}
	}

	// else not yet in the file but in either of the two caches

	db.toBeDeleted[id] = Flag{} // set map to empty value as a flag indicating the item is to be deleted
	// remove the item from keyMap

	// keyMap contains lists of item ids, which share the same keyHash value, due to hashing collisions
	// to remove keyHash <-> id assignment, one has to find the right Id in the lice
	idList := db.keyHashItems[keyHash]
	for i, candidateId := range idList {
		if candidateId == id {
			idList[i] = idList[len(idList)-1]
			db.keyHashItems[keyHash] = idList[:len(idList)-1]
			// idList = append(idList[:i], idList[i+1:]...)
			// db.keyHashItems[keyHash] = idList
			break
		}
	}

	db.ItemsCount--
	return nil
}

// deletes a db item identified with the provided db key
func (db *SimpleDb[T]) Delete(aKey string) (err error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	keyHash := hash.Get(aKey)
	ids, ok := db.keyHashItems[keyHash]
	if !ok {
		return &NotFoundError{}
	}
	var key string
	var id ID
	for _, id = range ids {
		key, _, err = db.getItem(id)
		if err == nil && key == aKey {
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

	var bytesWritten int64

	var tmpFile = filepath.Join(DbPath, "temp.sdb")

	if _, err := db.writeBuff.flush(db.file); err != nil { // flush write buffer
		return &DbInternalError{oper: "flushing cche"}
	}

	if err = db.file.Close(); err != nil {
		return &DbInternalError{oper: "closing: %w", err: err}
	}

	if len(db.toBeDeleted) != 0 { // if the database file needs to be reorganized
		if bytesWritten, err = db.reorganizeDbFile(tmpFile); err != nil {
			return &DbInternalError{oper: "reorganizing: %w", err: err}
		}
		if bytesWritten == 0 { // if the db file is empty - i.e. everything has been deleted
			if err := os.Remove(tmpFile); err != nil {
				return &DbInternalError{oper: "removing tmp file: %w", err: err}
			}
			return nil
		}

		if err := os.Remove(db.filePath); err != nil { // switch the temp file with  the datbase file
			return &DbInternalError{oper: "removing db file: %w", err: err}
		}

		if err := os.Rename(tmpFile, db.filePath); err != nil {
			return &DbInternalError{oper: "renaming tmp to db file: %w", err: err}
		}
	}

	return
}

func (db *SimpleDb[T]) reorganizeDbFile(tmpFile string) (bytesWritten int64, err error) {
	var (
		curpos int64
		header blockHeader
		src    *os.File
		dest   *os.File
	)
	// copy the database file to a temp file, while omitting deleted items

	if dest, err = openFile(tmpFile); err != nil {
		return 0, err
	}
	if src, err = openFile(db.filePath); err != nil {
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
		if _, delete := db.toBeDeleted[ID(header.Id)]; !delete {
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
				bytesWritten += int64(n)
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
		curpos int64
		lastId ID
		count  int
	)

	db.blockOffsets = make(map[ID]int64)
	var header blockHeader

loop:
	for {
		if _, err = db.file.Seek(int64(curpos), 0); err != nil {
			return err
		}
		if err = header.read(db.file); err != nil {
			if errors.Is(err, io.EOF) {
				break loop
			} else {
				return err
			}
		}

		db.blockOffsets[ID(header.Id)] = curpos   // updat offsets map
		db.keyHashItems[header.KeyHash] = append( // update kayhashmap
			db.keyHashItems[header.KeyHash],
			ID(header.Id))
		curpos += int64(header.Length) // update current position in the file
		if ID(header.Id) > lastId {    // keep track of the last id
			lastId = ID(header.Id)
		}
		count++
	}
	db.currentOffset = curpos // update database parameters
	db.ItemsCount = count
	db.maxId = lastId + 1 // value of the next ID to be generated
	return nil
}

// checks if the database contains an element with the given ID
func (db *SimpleDb[T]) contains(id ID) (ok bool) {
	_, ok = db.blockOffsets[id]
	return
}
