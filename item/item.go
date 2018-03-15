package item

import (
	"errors"
	"time"

	"github.com/pointc-io/sliced"
	"github.com/pointc-io/sliced/codec/gjson"
	"github.com/pointc-io/sliced/codec/sjson"
	"github.com/pointc-io/sliced/index/btree"
)

var (
	ErrNotJSON = errors.New("not json")
)

// Value represents a value in the DB
// It can be set to auto expire
// It can have secondary indexes
type ValueItem struct {
	Key     Key
	Value   string
	Expires int64
	Indexes []IndexItem
}

func (dbi *ValueItem) Get(path string) gjson.Result {
	return gjson.Get(dbi.Value, path)
}

// Set a field in the JSON document with a raw value
func (dbi *ValueItem) SetRaw(path string, rawValue string) (string, error) {
	return sjson.SetRaw(dbi.Value, path, rawValue)
}

// BTree key comparison.
func (dbi *ValueItem) Less(than btree.Item, ctx interface{}) bool {
	//return dbi.Key < than.(*Value).Key
	return dbi.Key.Less(than, ctx)
}

// expired evaluates id the value has expired. This will always return false when
// the value does not have `opts.ex` set to true.
func (dbi *ValueItem) expired() bool {
	return dbi.Expires > 0 && time.Now().Unix() > dbi.Expires
	//return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// expiresAt will return the time when the value will expire. When an value does
// not expire `maxTime` is used.
func (dbi *ValueItem) expiresAt() int64 {
	return dbi.Expires
}

func (dbi *ValueItem) Type() sliced.DataType {
	return dbi.Key.Type()
}

func (dbi *ValueItem) Match(pattern string) bool {
	return dbi.Key.Match(pattern)
}

func (dbi *ValueItem) LessThan(key Key) bool {
	return dbi.LessThan(key)
}

func (dbi *ValueItem) LessThanItem(than btree.Item, item *ValueItem) bool {
	return dbi.LessThanItem(than, item)
}
