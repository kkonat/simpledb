package simpledb

import (
	"testing"
	"time"
)

func TestBlock(t *testing.T) {
	block1 := NewBlock(0,
		uint64(time.Now().Unix()),
		[]byte("KeyKey"),
		[]byte("ValueValue"),
	)

	data := block1.getBytes()

	printBytes(data)

	block2 := &block{}
	block2.setBytes(data)

	printBytes(block2.getBytes())

	if block2.Id != block1.Id ||
		block2.Timestamp != block1.Timestamp ||
		block2.KeyHash != block1.KeyHash ||
		string(block2.key) != string(block1.key) ||
		string(block2.value) != string(block1.value) {
		t.Error("Data mismatch")
	}
}
