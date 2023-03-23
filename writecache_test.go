package simpledb

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
)

type checkData struct {
	id     ID
	key    string
	offset uint64
}

// Test buffer in real-life scenario
func TestBuff(t *testing.T) {
	const filename = "db\\testbuff.sdb"
	var (
		header      blockHeader
		curpos      uint64
		count       int
		err         error
		offset      uint64
		globalCheck []checkData = []checkData{}
		accumulated int
		flushes     int
		chkBo       []idOffset = []idOffset{}
		curOffset   uint64
	)

	// testing parameters
	const (
		N          = 60000
		rndSize    = 50
		flushLimit = 16536
	)

	//cleanup
	err = os.Remove(filename)
	if err != nil {
		panic("can't remove file, will not proceed")
	}

	// create write buffer
	file, _ := openFile(filename)
	wb := newWriteBuff(file)

	// store N items
	for i := 0; i < N; i++ {
		// generate dummy data
		id := ID(i)
		data := make([]byte, rndSize+rand.Intn(rndSize))
		key := []byte(fmt.Sprintf("Item %04d", id))

		// create block
		block := NewBlock(id, key, data)
		blockBytes := block.getBytes()
		noBytes := len(blockBytes)

		// append to the write buffer
		wb.grow(id, blockBytes)

		// store data for verification
		globalCheck = append(globalCheck,
			checkData{
				id:     id,
				key:    string(key),
				offset: offset,
			})
		chkBo = append(chkBo,
			idOffset{
				id,
				curOffset,
			})

		// do maths
		curOffset += uint64(noBytes)
		offset += uint64(noBytes)
		accumulated += noBytes

		// write buffered data when flushLimit exceeded
		if accumulated > flushLimit {
			bo, _ := wb.flush() // flush

			// verify if flush returns correct data
			for j := 0; j < len(bo); j++ {
				if bo[j].offset != chkBo[j].offset {
					t.Error(
						"fl", flushes,
						"item:", j,
						"bo is:", bo[j].offset,
						"exp:", chkBo[j].offset,
						"check:", globalCheck[j].offset)
					return
				}
			}
			flushes++

			// reset values for the next buffer
			curOffset = 0
			accumulated = 0
			chkBo = []idOffset{}
			wb.reset()
		}
	}
	wb.flush()
	file.Close()
	// finish saving the datafile

	fmt.Println("flushed", flushes, "times")

	// read file to mem
	file, _ = openFile(filename)
	blockOffsets := make(BlockOffsets)
loop:
	for {
		// read block headers
		if _, err = file.Seek(int64(curpos), 0); err != nil {
			t.Error("seek error :", curpos)
		}
		if err = header.read(file); err != nil {
			if errors.Is(err, io.EOF) {
				break loop
			} else {
				t.Error("error :", err)
				return
			}
		}
		// buiild offsets
		blockOffsets[ID(header.Id)] = curpos
		curpos += uint64(header.Length)

		count++
	}
	file.Close() // done reading

	// now verify if what's read is the same as what was saved
	for i := 0; i < N; i++ {
		id := globalCheck[i].id
		if blockOffsets[id] != globalCheck[i].offset {
			t.Error("i:", i, ",id", id, "is :", blockOffsets[id], "expected", globalCheck[i].offset)
			return
		}
	}
}
