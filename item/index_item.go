package item

import (
	"github.com/pointc-io/ipdb/index/btree"
	"github.com/pointc-io/ipdb/index/rtree"
)

//
// IndexItems do not need to be serialized since they
// can be recreated (rebuilt) from the index meta data
//
// Projected keys are converted to the appropriate value type.
// This favors scan speeds. Strings point to a slice of the value
// and are always constant size of a pointer and the string header.
// strings are favored over []byte due to it costing 8 bytes less memory.
// Go structs are 8 byte aligned.
type IndexItem interface {
	btree.Item
	rtree.Item

	// Internal
	index() *Index
	setIndex(idx *Index)
	// Internal

	Value() *ValueItem

	//IndexItemBase(idx *index, value *Value) Value

	//IsComposite() bool
	//ParseKeyBytes(raw string) Value
	//ParseComposite(raw ... string) Value
	//
	////ParseBytes(raw []byte) Value
	////ParseBytesComposite(raw... []byte) Value
	////ConvertFloat64(f float64) Value
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
	idx   *Index
	value *ValueItem
}
func (i *IndexItemBase) index() *Index {
	return i.idx
}
func (i *IndexItemBase) setIndex(index *Index) {
	i.idx = index
}
func (i *IndexItemBase) Value() *ValueItem {
	return i.value
}
// rtree.Value
func (i *IndexItemBase) Rect(ctx interface{}) (min []float64, max []float64) {
	return nil, nil
}
// btree.Value
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

// rtree.Value
func (r *RectItem) Rect(ctx interface{}) (min []float64, max []float64) {
	return r.Key.Min, r.Key.Max
}

type AnyItem struct {
	IndexItemBase
	Key Key
}


// btree.Value
func (k *AnyItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
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

// rtree.Value
func (k *StringItem) Rect(ctx interface{}) (min []float64, max []float64) {
	return nil, nil
}

// btree.Value
func (k *StringItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
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

// btree.Value
func (k *StringCaseInsensitiveItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
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


// btree.Value
func (k *IntItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
}

//
//
//
type FloatItem struct {
	IndexItemBase
	Key FloatKey
}

// btree.Value
func (k *FloatItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
}


//
//
//
type FloatDescItem struct {
	IndexItemBase
	Key FloatDescKey
}

// btree.Value
func (k *FloatDescItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
}
