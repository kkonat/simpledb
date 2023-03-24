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
	offset int64
}

// Test buffer in real-life scenario
func TestBuff(t *testing.T) {
	const filename = "db\\testbuff.sdb"
	var (
		header      blockHeader
		curpos      int64
		count       int
		err         error
		offset      int64
		globalCheck []checkData = []checkData{}
		accumulated int64
		flushes     int
		chkBo       []blockOffset = []blockOffset{}
		curOffset   int64
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
	wb := newWriteBuff()

	// store N items
	for i := 0; i < N; i++ {
		// generate dummy data
		id := ID(i)
		data := make([]byte, rndSize+rand.Intn(rndSize))
		key := fmt.Sprintf("Item %04d", id)

		// create block
		block := NewBlock(id, key, data)
		blockBytes := block.getBytes()
		noBytes := int64(len(blockBytes))

		// append to the write buffer
		wb.append(id, blockBytes)

		// store data for verification
		globalCheck = append(globalCheck,
			checkData{
				id:     id,
				key:    string(key),
				offset: offset,
			})
		chkBo = append(chkBo,
			blockOffset{
				id,
				curOffset,
			})

		// do maths
		curOffset += int64(noBytes)
		offset += int64(noBytes)
		accumulated += noBytes

		// write buffered data when flushLimit exceeded
		if accumulated > flushLimit {
			bo, _ := wb.flush(file) // flush

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
			chkBo = []blockOffset{}
			wb.reset()
		}
	}
	wb.flush(file)
	file.Close()
	// finish saving the datafile

	fmt.Println("flushed", flushes, "times")

	// read file to mem
	file, _ = openFile(filename)
	blockOffsets := make(map[ID]int64)
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
		curpos += int64(header.Length)

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
