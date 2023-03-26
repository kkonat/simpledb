package simpledb

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
)

// processes relative dirs and returns final filepath
func getFilepath(filename string) string {
	dbDataFile := filepath.Clean(filename)
	dir, file := filepath.Split(dbDataFile)
	dataFilePath := filepath.Join(dir, DbPath, file+DbExt)
	return dataFilePath
}

// opens the file, used for keeping track of the open/rw/create mode
func openFile(path string) (file *os.File, err error) {
	file, err = os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	return
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

// or for fun, let's try this function
// http://www.azillionmonkeys.com/qed/hash.html

var (
	letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func getRandomLetters(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func genRandomSequence(N int) []int {
	if N == math.MaxInt {
		panic("sequence too long")
	}
	const sentinel = math.MaxInt
	var seq = make([]int, N)
	for i := 0; i < len(seq); i++ {
		seq[i] = sentinel
	}
	for i := 0; i < N; i++ {
		p := rand.Intn(N)
		if seq[p] == sentinel {
			seq[p] = i
		} else {
			for ; p < N && seq[p] != sentinel; p++ {
			}
			if p < N {
				seq[p] = i
			} else {
				for p = 0; p < N && seq[p] != sentinel; p++ {
				}
				if p < N {
					seq[p] = i
				} else {
					panic("No place left")
				}
			}
		}
	}
	return seq
}
