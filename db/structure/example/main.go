package main

import (
	"github.com/pointc-io/ipdb/db/structure"
	"fmt"
	"encoding/binary"
	"unsafe"
)

func main() {
	tree := btree.New(64, nil)
	_ = tree

	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, 10)

	fmt.Println(binary.LittleEndian.Uint32(b))

	b = []byte("hi")
	in := *(*string)(unsafe.Pointer(&b))
	fmt.Println(in)

	var item *btree.TreeItem
	for i := 5; i < 10; i++ {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(i))
		item = &btree.TreeItem{
			Key:   buf,
			Value: []byte("Hi"),
		}
		tree.ReplaceOrInsert(item)
	}

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(8))

	tree.DescendLessOrEqual(&btree.TreeItem{Key: buf}, func(i btree.Item) bool {
		fmt.Println(i)
		return true
	})

	tree.AscendGreaterOrEqual(&btree.TreeItem{Key: buf}, func(i btree.Item) bool {
		fmt.Println(i)
		return true
	})

	binary.LittleEndian.PutUint32(buf, 1)
	item = &btree.TreeItem{
		Key:   buf,
		Value: []byte("Hi"),
	}
	tree.ReplaceOrInsert(item)


	//it := tree.ReplaceOrInsert(item)
	//
	//fmt.Println(it)
	//fmt.Println(item)
}
