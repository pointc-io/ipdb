package sorted

import (
	"github.com/pointc-io/ipdb/db/data/btree"
	"github.com/pointc-io/ipdb/db/data/rtree"
)

//
// IndexItems do not need to be serialized since they
// can be recreated (rebuilt) from the index meta data
//
type IndexItem interface {
	btree.Item
	rtree.Item

	// Internal
	index() *index
	setIndex(idx *index)
	// Internal

	Item() *Item

	//indexItem(idx *index, item *Item) IndexItem

	//IsComposite() bool
	//Parse(raw string) IndexItem
	//ParseComposite(raw ... string) IndexItem
	//
	////ParseBytes(raw []byte) IndexItem
	////ParseBytesComposite(raw... []byte) IndexItem
	////ConvertFloat64(f float64) IndexItem
	//Type() DataType

	//// Number of keys
	//Keys() int
	//// First key
	//Key() string
	//// Key at a particular index
	//// Only uses for composite indexes
	//KeyAt(index int) string
	//// First key as an int64 if possible
	//// Otherwise, 0
	//Int64() int64
	//// First key as a float64 if possible
	//// Otherwise, 0
	//Float64() float64
	////
	//Int64At(index int) int64
	////
	//Float64At(index int) float64
}

//
// Base struct
//
type indexItem struct {
	idx  *index
	item *Item
}

func (i *indexItem) index() *index {
	return i.idx
}

func (i *indexItem) setIndex(index *index) {
	i.idx = index
}

func (i *indexItem) Item() *Item {
	return i.item
}

// rtree.Item
func (i *indexItem) Rect(ctx interface{}) (min []float64, max []float64) {
	return nil, nil
}

// btree.Item
func (i *indexItem) Less(than btree.Item, ctx interface{}) bool {
	return false
}

//
//
//
//
//
//
//
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

//
//
//
//
//
//
//
//
//
//
// Key of arbitrary bytes
type stringKey struct {
	indexItem
	key StringKey
}

// rtree.Item
func (k *stringKey) Rect(ctx interface{}) (min []float64, max []float64) {
	return nil, nil
}

// btree.Item
func (k *stringKey) Less(than btree.Item, ctx interface{}) bool {
	return k.key.LessThanItem(than, k.item)
}

//
//
//
//
//
//
//

// Key of arbitrary bytes
type ciStringKey struct {
	stringKey
}

// btree.Item
func (k *ciStringKey) Less(than btree.Item, ctx interface{}) bool {
	return k.key.LessThanItem(than, k.item)
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

//
//
//
//
//
//
//

type compositeKey2Prototype struct {
	prototypes []IndexItem
}

func (i *compositeKey2Prototype) ParseComposite(raw ... string) IndexItem {
	if len(i.prototypes) < len(raw) {
		for i, prototype := range i.prototypes {
			_ = i
			_ = prototype
		}
	}
	return &indexItem{}
}

func (k *compositeKey2Prototype) Parse(raw string) IndexItem {
	return &stringKey{}
}
func (k *compositeKey2Prototype) ParseBytesComposite(raw ... []byte) IndexItem {
	return &stringKey{}
}

//
//
//
//
//
//
//

type compositeKey2 struct {
	indexItem
	key  string
	key2 string
}

func (k *compositeKey2) Parse(raw string) IndexItem {
	return &compositeKey2{
		key: raw,
	}
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

func (i *compositeKey2) Keys() int {
	return 2
}
func (i *compositeKey2) Key() string {
	return i.key
}
func (i *compositeKey2) Int64() int64 {
	return 0
}
func (i *compositeKey2) Float64() float64 {
	return 0
}
func (i *compositeKey2) KeyAt(index int) string {
	switch index {
	case 0:
		return i.key
	case 1:
		return i.key2
	default:
		return ""
	}
}
func (i *compositeKey2) Int64At(index int) int64 {
	return 0
}
func (i *compositeKey2) Float64At(index int) float64 {
	return 0
}



//
//
//
//
//
//
//

type intKey struct {
	indexItem
	key IntKey
}


// btree.Item
func (k *intKey) Less(than btree.Item, ctx interface{}) bool {
	return k.key.Less(than, ctx)

	//switch t := than.(type) {
	//case *intKey:
	//	if k.key < t.key {
	//		return true
	//	} else if k.key > t.key {
	//		return false
	//	} else {
	//		if k.item == nil {
	//			return t.item != nil
	//		} else if t.item == nil {
	//			return k.item != nil
	//		} else {
	//			return k.item.Key < t.item.Key
	//		}
	//	}
	//
	//case IntKey:
	//	return k.key < t.Value
	//
	//case *IntKey:
	//	return k.key < t.Value
	//
	//case *floatKey:
	//	ti := int64(t.key)
	//	if k.key < ti {
	//		return true
	//	} else if k.key > ti {
	//		return false
	//	} else {
	//		if k.item == nil {
	//			return t.item != nil
	//		} else if t.item == nil {
	//			return k.item != nil
	//		} else {
	//			return k.item.Key < t.item.Key
	//		}
	//	}
	//
	//case FloatKey:
	//	return k.key < t.AsInt64()
	//case *FloatKey:
	//	return k.key < t.AsInt64()
	//
	//case StringKey:
	//	return true
	//case *StringKey:
	//	return true
	//
	//case *stringKey:
	//	return true
	//
	//case *NilKey:
	//	return false
	//case NilKey:
	//	return false
	//case nil:
	//	return false
	//}
	//return false
}

//
//
//
type floatKey struct {
	indexItem
	key FloatKey
}

// btree.Item
func (k *floatKey) Less(than btree.Item, ctx interface{}) bool {
	return k.key.LessThanItem(than, k.item)
}
