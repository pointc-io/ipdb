package item

import (
	"github.com/pointc-io/sliced/index/btree"
	"github.com/pointc-io/sliced/index/rtree"
)

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

	//indexItem(idx *index, value *Value) Value

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
	//K() string
	//// K at a particular index
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
	idx   *Index
	value *ValueItem
}

func (i *indexItem) index() *Index {
	return i.idx
}
func (i *indexItem) setIndex(index *Index) {
	i.idx = index
}
func (i *indexItem) Value() *ValueItem {
	return i.value
}

// rtree.Value
func (i *indexItem) Rect(ctx interface{}) (min []float64, max []float64) {
	return nil, nil
}

// btree.Value
func (i *indexItem) Less(than btree.Item, ctx interface{}) bool {
	return false
}

//
//
//
// Nil
//
//
//

type nilItem struct {
	indexItem
}

func (k *nilItem) Less(than btree.Item, ctx interface{}) bool {
	return true
}

//
//
//
// False
//
//
//

type falseItem struct {
	indexItem
}

func (k *falseItem) Less(than btree.Item, ctx interface{}) bool {
	return False.Less(than, k.value)
}

//
//
//
// True
//
//
//

type trueItem struct {
	indexItem
}

func (k *trueItem) Less(than btree.Item, ctx interface{}) bool {
	return True.Less(than, k.value)
}

//
//
//
// Rect
//
//
//
// Rect key is for the RTree
type rectItem struct {
	indexItem
	key Rect
}

func (i *rectItem) Key() Key {
	return i.key
}

// rtree.Value
func (r *rectItem) Rect(ctx interface{}) (min []float64, max []float64) {
	return r.key.Min, r.key.Max
}

//
type AnyItem struct {
	indexItem
	key Key
}

func (k *AnyItem) Less(than btree.Item, ctx interface{}) bool {
	return k.key.LessThanItem(than, k.value)
}

//
//
//
// String
//
//
//

type stringItem struct {
	indexItem
	K StringKey
}

func (k *stringItem) Less(than btree.Item, ctx interface{}) bool {
	return k.K.LessThanItem(than, k.value)
}

//
//
//
// String in descending order
//
//
//

type stringDescItem struct {
	indexItem
	K StringDescKey
}

func (k *stringDescItem) Less(than btree.Item, ctx interface{}) bool {
	return k.K.LessThanItem(than, k.value)
}

//
//
//
// String Case Insensitive
//
//
//

type stringCIItem struct {
	indexItem
	Key StringCIKey
}

func (k *stringCIItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
}

//
//
//
// String Case Insensitive in descending order
//
//
//

type stringCIDescItem struct {
	indexItem
	Key StringCIDescKey
}

func (k *stringCIDescItem) Less(than btree.Item, ctx interface{}) bool {
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
// Int -> IntKey
//
//
//

type intItem struct {
	indexItem
	Key IntKey
}

func (k *intItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
}

type intDescItem struct {
	indexItem
	Key IntDescKey
}

func (k *intDescItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
}

//
//
//
// Float -> FloatKey
//
//
//

type floatItem struct {
	indexItem
	Key FloatKey
}

func (k *floatItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
}

type floatDescItem struct {
	indexItem
	Key FloatDescKey
}

func (k *floatDescItem) Less(than btree.Item, ctx interface{}) bool {
	return k.Key.LessThanItem(than, k.value)
}

//
//
//
// Composite
//
//
//

type composite2Item struct {
	indexItem
	K  Key2
}

func (i *composite2Item) Less(than btree.Item, ctx interface{}) bool {
	return i.K.LessThanItem(than, i.value)
}

//type composite3Item struct {
//	indexItem
//	K  Key3
//}
//
//func (i *composite3Item) Less(than btree.Item, ctx interface{}) bool {
//	return i.K.LessThanItem(than, i.value)
//}
