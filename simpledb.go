package simpledb

import (
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	dbFileExt  = ".sdb"
	idxFileExt = ".idx"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

// Index holds offsets to individual data entries in the database file
type Index map[int64]int64

type SimpleDb struct {
	dataFile string
	index    Index
}
type DbItem struct {
	id       int64
	data     []byte
	lastUsed time.Time
}

func New(filename string) (*SimpleDb, error) {

	fname := filepath.Clean(filename)
	if _, err := os.Stat(fname); err == nil {
		log.Debug("Using '%s' (database already exists)\n", dir)

	}
}

func Write(data any) (id int64, err error) {

}

func Get(id int64) (any, error) {

}

func purgeOld() error {}
