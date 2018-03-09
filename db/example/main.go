package main

import (
	"github.com/tidwall/btree"
	"fmt"
)

// Default number of btree degrees
const btreeDegrees = 64

type dbItem struct {
	key     []byte
	value   []byte
	compare []byte
}

func (i *dbItem) Less(than btree.Item, ctx interface{}) bool {
	return true
}

func main() {
	btr := btree.New(btreeDegrees, nil)
	item := &dbItem{key: []byte("p:0"), value: []byte("{\"name\":\"ACME\"}")}
	i := btr.ReplaceOrInsert(item)
	fmt.Println(item)
	fmt.Println(i)
}
