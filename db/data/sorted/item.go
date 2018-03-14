package sorted

import (
	"errors"
	"time"

	"github.com/pointc-io/ipdb/db/data/btree"
	"github.com/pointc-io/ipdb/codec/gjson"
	"github.com/pointc-io/ipdb/codec/sjson"
	"github.com/pointc-io/ipdb/db/data"
)

var (
	ErrNotJSON = errors.New("not json")
)

// Item represents a value in the DB
// It can be set to auto expire
// It can have secondary indexes
type Item struct {
	Key     Key
	Value   string
	Expires int64

	indexes []IndexItem
}

func (dbi *Item) Get(path string) gjson.Result {
	return gjson.Get(dbi.Value, path)
}

// Set a field in the JSON document with a raw value
func (dbi *Item) SetRaw(path string, rawValue string) (string, error) {
	return sjson.SetRaw(dbi.Value, path, rawValue)
}

// BTree key comparison.
func (dbi *Item) Less(than btree.Item, ctx interface{}) bool {
	//return dbi.Key < than.(*Item).Key
	return dbi.Key.Less(than, ctx)
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

func (dbi *Item) Type() data.DataType {
	return dbi.Key.Type()
}

func (dbi *Item) Match(pattern string) bool {
	return dbi.Key.Match(pattern)
}

func (dbi *Item) LessThan(key Key) bool {
	return dbi.LessThan(key)
}

func (dbi *Item) LessThanItem(than btree.Item, item *Item) bool {
	return dbi.LessThanItem(than, item)
}
