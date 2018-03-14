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
	index() *SetIndex
	setIndex(idx *SetIndex)
	// Internal

	Item() *Item

	//IndexItemBase(idx *index, item *Item) IndexItem

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
type IndexItemBase struct {
	idx  *SetIndex
	item *Item
}
func (i *IndexItemBase) index() *SetIndex {
	return i.idx
}
func (i *IndexItemBase) setIndex(index *SetIndex) {
	i.idx = index
}
func (i *IndexItemBase) Item() *Item {
	return i.item
}
// rtree.Item
func (i *IndexItemBase) Rect(ctx interface{}) (min []float64, max []float64) {
	return nil, nil
}
// btree.Item
func (i *IndexItemBase) Less(than btree.Item, ctx interface{}) bool {
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
type RectItem struct {
	IndexItemBase
	Key Rect
}

// rtree.Item
func (r *RectItem) Rect(ctx interface{}) (min []float64, max []float64) {
	return r.Key.Min, r.Key.Max
}

type AnyItem struct {
	IndexItemBase
	Key Key
}


// btree.Item
func (k *AnyItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.item)
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
type StringItem struct {
	IndexItemBase
	Key StringKey
}

// rtree.Item
func (k *StringItem) Rect(ctx interface{}) (min []float64, max []float64) {
	return nil, nil
}

// btree.Item
func (k *StringItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.item)
}

//
//
//
//
//
//
//

// Key of arbitrary bytes
type StringCaseInsensitiveItem struct {
	StringItem
}

// btree.Item
func (k *StringCaseInsensitiveItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.item)
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

type IntItem struct {
	IndexItemBase
	Key IntKey
}


// btree.Item
func (k *IntItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.item)
}

//
//
//
type FloatItem struct {
	IndexItemBase
	Key FloatKey
}

// btree.Item
func (k *FloatItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.item)
}
