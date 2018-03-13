package sorted

import (
	"time"
	"strconv"

	"github.com/pointc-io/ipdb/db/data/btree"
	"github.com/pointc-io/ipdb/db/data/rtree"
	"github.com/pointc-io/ipdb/codec/gjson"
	"github.com/pointc-io/ipdb/codec/sjson"
	"github.com/murlokswarm/errors"
	"github.com/pointc-io/ipdb/db/data/grect"
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

func (dbi *Item) AppendValue(buf []byte) []byte {
	return buf
}

func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(count), 10)...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = append(buf, strconv.FormatInt(int64(len(s)), 10)...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// writeSetTo writes an item as a single SET record to the a bufio Writer.
func (dbi *Item) writeSetTo(buf []byte) []byte {
	if dbi.Expires > 0 {
		ex := dbi.Expires
		buf = appendArray(buf, 5)
		buf = appendBulkString(buf, "SET")
		buf = appendBulkString(buf, dbi.Key)
		buf = appendBulkString(buf, string(dbi.AppendValue(nil)))
		buf = appendBulkString(buf, "EX")
		buf = appendBulkString(buf, strconv.FormatUint(uint64(ex), 10))
	} else {
		buf = appendArray(buf, 3)
		buf = appendBulkString(buf, "SET")
		buf = appendBulkString(buf, dbi.Key)
		buf = appendBulkString(buf, string(dbi.AppendValue(nil)))
	}
	return buf
}

// writeSetTo writes an item as a single DEL record to the a bufio Writer.
func (dbi *Item) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkString(buf, dbi.Key)
	return buf
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

//
// IndexItems do not need to be serialized since they
// can be recreated (rebuilt) from the index meta data
//
type IndexItem interface {
	btree.Item
	rtree.Item

	index() *index
	setIndex(idx *index)
}

type IndexFactory func(idx *index, item *Item) IndexItem
type IndexValue func(item *Item) (string, bool)
type IndexSpatialValue func(item *Item) ([]float64, []float64, bool)

func StringValue() func(item *Item) (string, bool) {
	return func(item *Item) (string, bool) {
		return item.Value, true
	}
}

func NumberValue() func(item *Item) (string, bool) {
	return func(item *Item) (string, bool) {
		num, err := strconv.ParseFloat(item.Value, 64)
		if err != nil {
			return "", false
		}
		return FloatToString(num), true
	}
}

func RectValue(item *Item) ([]float64, []float64, bool) {
	rect := grect.Get(item.Value)
	if len(rect.Min) == 0 && len(rect.Max) == 0 {
		return nil, nil, false
	}
	return rect.Min, rect.Max, true
}

func JSONString(path string) func(item *Item) (string, bool) {
	return func(item *Item) (string, bool) {
		result := gjson.Get(item.Value, path)

		switch result.Type {
		default:
			return "", false
		case gjson.String:
		}

		return result.Str, true
	}
}

func JSONNumber(path string) func(item *Item) (string, bool) {
	return func(item *Item) (string, bool) {
		result := gjson.Get(item.Value, path)

		switch result.Type {
		default:
			return "", false
		case gjson.Number:
		}

		return FloatToString(result.Num), true
	}
}

func JSONRect(path string) IndexSpatialValue {
	return func(item *Item) ([]float64, []float64, bool) {
		result := gjson.Get(item.Value, path)

		var min, max []float64
		if result.Type == gjson.String {
			rect := grect.Get(result.Str)
			min = rect.Min
			max = rect.Max
		} else if result.Type == gjson.JSON {
			if result.Raw[0] == '[' {
				rect := grect.Get(result.Raw)
				min = rect.Min
				max = rect.Max
			} else {
				return nil, nil, false
			}
		} else {
			return nil, nil, false
		}

		if min == nil && max == nil {
			return nil, nil, false
		}

		return min, max, true
	}
}

func JSONWildcard(path string) IndexValue {
	return func(item *Item) (string, bool) {
		result := gjson.Get(item.Value, path)

		switch result.Type {
		case gjson.String:
			return result.Str, true
		case gjson.Null:
			return "", true
		case gjson.JSON:
			return "", false
		case gjson.True:
			return string([]byte{1}), true
		case gjson.False:
			return string([]byte{0}), true
		case gjson.Number:
			return FloatToString(result.Num), true
		default:
			return result.Raw, true
		}
	}
}

func SpatialKey(val IndexSpatialValue) IndexFactory {
	return func(idx *index, item *Item) IndexItem {
		min, max, ok := val(item)
		if !ok {
			return nil
		}
		return &rectKey{
			indexItem: indexItem{
				idx:  idx,
				item: item,
			},
			min: min,
			max: max,
		}
	}
}

func Key(val IndexValue) IndexFactory {
	return func(idx *index, item *Item) IndexItem {
		i, ok := val(item)
		if !ok {
			return nil
		}
		return &stringKey{
			indexItem: indexItem{
				idx:  idx,
				item: item,
			},
			key: i,
		}
	}
}

func CompositeKey(val... IndexValue) IndexFactory {
	switch len(val) {
	case 0:
		panic("need at least 1")
	case 1:
		return Key(val[0])
	default:
		val1 := val[0]
		val2 := val[1]
		return func(idx *index, item *Item) IndexItem {
			key, ok := val1(item)
			if !ok {
				return nil
			}
			key2, ok := val2(item)
			return &compositeKey2{
				indexItem: indexItem{
					idx:  idx,
					item: item,
				},
				key:  key,
				key2: key2,
			}
		}
	}
}


//
//
//
type indexItem struct {
	idx  *index
	item *Item
}

func (i *indexItem) index() *index {
	return i.idx
}

func (i *indexItem) setIndex(idx *index) {
	i.idx = idx
}

// rtree.Item
func (i *indexItem) Rect(ctx interface{}) (min []float64, max []float64) {
	return nil, nil
}

// btree.Item
func (i *indexItem) Less(than btree.Item, ctx interface{}) bool {
	return false
}

// Rect key is for the RTree
type rectKey struct {
	indexItem
	min []float64
	max []float64
}

// rtree.Item
func (r *rectKey) Rect(ctx interface{}) (min []float64, max []float64) {
	return r.min, r.max
}

// btree.Item
func (k *rectKey) Less(than btree.Item, ctx interface{}) bool {
	t := than.(*rectKey)
	if k.item == nil {
		return t.item != nil
	} else if t.item == nil {
		return k.item != nil
	} else {
		return k.item.Key < t.item.Key
	}
	//if k.key < t.key {
	//	return true
	//} else if k.key > t.key {
	//	return false
	//} else {
	//	if k.item == nil {
	//		return t.item != nil
	//	} else if t.item == nil {
	//		return k.item != nil
	//	} else {
	//		return k.item.Key < t.item.Key
	//	}
	//}
}

// Key of arbitrary bytes
type stringKey struct {
	indexItem
	key string
}

// rtree.Item
func (r *stringKey) Rect(ctx interface{}) (min []float64, max []float64) {
	return nil, nil
}

// btree.Item
func (k *stringKey) Less(than btree.Item, ctx interface{}) bool {
	t := than.(*stringKey)
	if k.key < t.key {
		return true
	} else if k.key > t.key {
		return false
	} else {
		if k.item == nil {
			return t.item != nil
		} else if t.item == nil {
			return k.item != nil
		} else {
			return k.item.Key < t.item.Key
		}
	}
}

func stringLessInsensitive(a, b string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] >= 'A' && a[i] <= 'Z' {
			if b[i] >= 'A' && b[i] <= 'Z' {
				// both are uppercase, do nothing
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			} else {
				// a is uppercase, convert a to lowercase
				if a[i]+32 < b[i] {
					return true
				} else if a[i]+32 > b[i] {
					return false
				}
			}
		} else if b[i] >= 'A' && b[i] <= 'Z' {
			// b is uppercase, convert b to lowercase
			if a[i] < b[i]+32 {
				return true
			} else if a[i] > b[i]+32 {
				return false
			}
		} else {
			// neither are uppercase
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
	}
	return len(a) < len(b)
}


type compositeKey2 struct {
	indexItem
	key  string
	key2 string
}

// btree.Item
func (k *compositeKey2) Less(than btree.Item, ctx interface{}) bool {
	t := than.(*compositeKey2)
	if k.key < t.key {
		return true
	} else if k.key > t.key {
		return false
	} else {
		if k.key2 < t.key2 {
			return true
		} else if k.key2 > t.key2 {
			return false
		} else {
			if k.item == nil {
				return t.item != nil
			} else if t.item == nil {
				return k.item != nil
			} else {
				return k.item.Key < t.item.Key
			}
		}
	}
}

type compositeKey3 struct {
	indexItem
	key  string
	key2 string
	key3 string
}

// btree.Item
func (k *compositeKey3) Less(than btree.Item, ctx interface{}) bool {
	t := than.(*compositeKey3)
	if k.key < t.key {
		return true
	} else if k.key > t.key {
		return false
	} else {
		if k.key2 < t.key2 {
			return true
		} else if k.key2 > t.key2 {
			return false
		} else {
			if k.key3 < t.key3 {
				return true
			} else if k.key3 > t.key3 {
				return false
			} else {
				if k.item == nil {
					return t.item != nil
				} else if t.item == nil {
					return k.item != nil
				} else {
					return k.item.Key < t.item.Key
				}
			}
		}
	}
}

type compositeKey4 struct {
}

type compositeKey5 struct {
}
