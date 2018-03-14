package sorted

import (
	"errors"
	"time"

	"github.com/pointc-io/ipdb/db/data/btree"
	"github.com/pointc-io/ipdb/codec/gjson"
	"github.com/pointc-io/ipdb/codec/sjson"
)

type DataType uint8

const (
	String    DataType = 0
	Float     DataType = 1
	Integer   DataType = 2
	Rectangle DataType = 3

	HyperLogLog DataType = 8
	JSON        DataType = 9
	MsgPack     DataType = 10
	List        DataType = 11
	Hash        DataType = 12

	IndexString    DataType = 0
	IndexFloat     DataType = 1
	IndexInteger   DataType = 2
	IndexRectangle DataType = 3
)

var (
	ErrNotJSON = errors.New("not json")
)

// Item represents a value in the DB
// It can be set to auto expire
// It can have secondary indexes
type Item struct {
	Type    DataType
	Key     string
	Value   string
	Expires int64

	indexes []IndexItem
}

func (dbi *Item) Get(path string) gjson.Result {
	if dbi.Type != JSON {
		return gjson.Result{}
	}
	return gjson.Get(dbi.Value, path)
}

// Set a field in the JSON document with a raw value
func (dbi *Item) SetRaw(path string, rawValue string) (string, error) {
	if dbi.Type != JSON {
		return "", ErrNotJSON
	}
	return sjson.SetRaw(dbi.Value, path, rawValue)
}

// BTree key comparison.
func (dbi *Item) Less(than btree.Item, ctx interface{}) bool {
	return dbi.Key < than.(*Item).Key
}

// expired evaluates id the item has expired. This will always return false when
// the item does not have `opts.ex` set to true.
func (dbi *Item) expired() bool {
	return dbi.Expires > 0 && time.Now().Unix() > dbi.Expires
	//return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// expiresAt will return the time when the item will expire. When an item does
// not expire `maxTime` is used.
func (dbi *Item) expiresAt() int64 {
	return dbi.Expires
}
