package simpledb

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	dbPath = "./db"
	dbExt  = ".json"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

type DbItemID int32

// type Hashmap map[int64][]DbItemID
type OffsetsData map[DbItemID]int64
type SimpleDb[T any] struct {
	infoFilePath string `json:"-"`

	dataFilePath string   `json:"-"`
	dataFile     *os.File `json:"-"`

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

func New[T any](filename string) (db *SimpleDb[T], err error) {

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
	} else {
		db = &SimpleDb[T]{} // create new database
		db.Offsets = make(OffsetsData)
	}
	db.dataFile, err = openDbFile(dataFilePath) // open data file for reading and writing
	db.infoFilePath = infoFilePath
	db.dataFilePath = dataFilePath
	return
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

func (db *SimpleDb[T]) getNewId() (id DbItemID) {
	id = db.ItemCounter
	db.ItemCounter++
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

	id = db.getNewId()

	item := DbItem[T]{
		Id:       db.ItemCounter,
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

	db.Offsets[id] = db.CurrOffset
	db.CurrOffset += int64(bytesWritten)

	return
}

func (db *SimpleDb[T]) Get(id DbItemID) (rd *DbItem[T], err error) {
	seek, ok := db.Offsets[id]

	if !ok {
		return nil, errors.New("item not found")
	}

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
	return readData, err
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

func unmarshalAny[T any](bytes []byte, out *T) error {
	if err := json.Unmarshal(bytes, out); err != nil {
		return err
	}
	return nil
}

func (db *SimpleDb[T]) Flush() error {
	var err error
	db.dataFile.Close()
	db.dataFile, err = openDbFile(db.dataFilePath) // reopen
	return err
}

// Helper functions
func openDbFile(path string) (file *os.File, err error) {
	file, err = os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	return
}
