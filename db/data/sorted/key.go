//
//
//
package sorted

import (
	"math"
	"strconv"

	"github.com/pointc-io/ipdb/db/data/btree"
	"github.com/pointc-io/ipdb/codec/gjson"
)

var (
	IntMinKey = IntKey{-math.MaxInt64}
	IntMaxKey = IntKey{math.MaxInt64}
	FloatMinKey = FloatKey{-math.MaxFloat64}
	FloatMaxKey = FloatKey{math.MaxFloat64}
)

// Parses an unknown key without any hints and converts
// to the most appropriate Key type.
func Parse(from []byte) Key {
	if from == nil {
		return NilKey{}
	}

	str := string(from)
	l := len(str)
	for i := 0; i < l; i++ {
		switch str[i] {
		case '-', '+':
			for i = i + 1; i < l; i++ {
				switch str[i] {
				case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				case '.':
					for i = i + 1; i < l; i++ {
						switch str[i] {
						case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
						default:
							return StringKey{str}
						}
					}

					// Try float.
					f, err := strconv.ParseFloat(str, 64)
					if err != nil {
						return StringKey{str}
					} else {
						return FloatKey{f}
					}
				default:
					return StringKey{str}
				}
			}
			// Try int64.
			f, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return StringKey{str}
			} else {
				return IntKey{f}
			}

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':

		case '.':
			for i = i + 1; i < l; i++ {
				switch str[i] {
				case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				default:
					return StringKey{str}
				}
			}

			// Try float.
			f, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return StringKey{str}
			} else {
				return FloatKey{f}
			}
		default:
			return StringKey{str}
		}
	}
	// Try int64.
	f, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return StringKey{str}
	} else {
		return IntKey{f}
	}
}

// Parses with a String type hint
func ParseString(from []byte) Key {
	if from == nil {
		return NilKey{}
	} else {
		return StringKey{Value: string(from)}
	}
}

// Parses with a Float type hint
func ParseFloat(from []byte) Key {
	v, err := strconv.ParseFloat(string(from), 64)
	if err != nil {
		return NilKey{}
	} else {
		return FloatKey{Value: v}
	}
}

// Parses with an Int type hint
func ParseInt(from []byte) Key {
	v, err := strconv.ParseInt(string(from), 10, 64)
	if err != nil {
		return NilKey{}
	} else {
		return IntKey{Value: v}
	}
}

// Parses with a Date type hint
func ParseDate(from []byte) Key {
	return NilKey{}
}

func JSONToKey(result gjson.Result) Key {
	switch result.Type {
	// Null is a null json value
	case gjson.Null:
		return NilKey{}
		// False is a json false boolean
	case gjson.False:
		return FalseKey{}
		// Number is json number
	case gjson.Number:
		return FloatKey{result.Num}
		// String is a json string
	case gjson.String:
		return StringKey{result.Str}
		// True is a json true boolean
	case gjson.True:
		return TrueKey{}
		// JSON is a raw block of JSON
	case gjson.JSON:
		return StringKey{result.Raw}
	}
	return NilKey{}
}

type Key interface {
	btree.Item
	IsNil() bool

	LessThan(key Key) bool

	LessThanItem(than btree.Item, item *Item) bool
}

//
//
//
type NilKey struct {
}

func (k NilKey) IsNil() bool {
	return true
}
func (k NilKey) Less(than btree.Item, ctx interface{}) bool {
	return true
}
func (k NilKey) LessThan(than Key) bool {
	return true
}
func (k NilKey) LessThanItem(than btree.Item, item *Item) bool {
	return true
}

//
//
//
type TrueKey struct{}

func (k TrueKey) IsNil() bool {
	return false
}
func (k TrueKey) Less(than btree.Item, ctx interface{}) bool {
	return true
}
func (k TrueKey) LessThan(than Key) bool {
	return true
}
func (k TrueKey) LessThanItem(than btree.Item, item *Item) bool {
	return true
}

//
//
//
type FalseKey struct{}

func (k FalseKey) IsNil() bool {
	return false
}
func (k FalseKey) Less(than btree.Item, ctx interface{}) bool {
	return true
}
func (k FalseKey) LessThan(than Key) bool {
	return true
}
func (k FalseKey) LessThanItem(than btree.Item, item *Item) bool {
	return true
}

type StringMinKey struct {
	StringKey
}

type StringMaxKey struct {
	StringKey
}

//
//
//
type StringKey struct {
	Value string
}

func (k StringKey) IsNil() bool {
	return false
}
func LessCaseInsensitive(a, b string) bool {
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
func (k StringKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case StringKey:
		return k.Value < t.Value
	case *StringKey:
		return k.Value < t.Value
	case *stringKey:
		return k.Value < t.key.Value
	}
	return false
}
func (k StringKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case StringKey:
		return k.Value < t.Value
	case *StringKey:
		return k.Value < t.Value
	}
	return false
}
func (k StringKey) LessThanItem(than btree.Item, item *Item) bool {
	switch t := than.(type) {
	case StringKey:
		return k.Value < t.Value
	case *StringKey:
		return k.Value < t.Value
	case *stringKey:
		if k.Value < t.key.Value {
			return true
		} else if k.Value > t.key.Value {
			return false
		} else {
			if item == nil {
				return t.item != nil
			} else if t.item == nil {
				return true
			} else {
				return item.Key < t.item.Key
			}
		}
	case *ciStringKey:
		if k.Value < t.key.Value {
			return true
		} else if k.Value > t.key.Value {
			return false
		} else {
			if item == nil {
				return t.item != nil
			} else if t.item == nil {
				return true
			} else {
				return item.Key < t.item.Key
			}
		}

	case IntKey, *IntKey, *intKey, FloatKey, *FloatKey, *floatKey:
		return false
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
}

type StringCIKey struct {
	StringKey
}

func (k StringCIKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case StringCIKey:
		return LessCaseInsensitive(k.Value, t.Value)
	case *StringCIKey:
		return LessCaseInsensitive(k.Value, t.Value)
	case StringKey:
		return LessCaseInsensitive(k.Value, t.Value)
	case *StringKey:
		return LessCaseInsensitive(k.Value, t.Value)
	case *stringKey:
		return LessCaseInsensitive(k.Value, t.key.Value)
	case *ciStringKey:
		return LessCaseInsensitive(k.Value, t.key.Value)
	}
	return false
}
func (k StringCIKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case StringKey:
		return k.Value < t.Value
	case *StringKey:
		return k.Value < t.Value
	case StringCIKey:
		return LessCaseInsensitive(k.Value, t.Value)
	case *StringCIKey:
		return LessCaseInsensitive(k.Value, t.Value)
	}
	return false
}

type IntKey struct {
	Value int64
}

func (k IntKey) IsNil() bool {
	return false
}
func (k IntKey) AsFloat64() float64 {
	return float64(k.Value)
}
func (k IntKey) AsInt64() int64 {
	return k.Value
}
func (k IntKey) AsUInt64() uint64 {
	return uint64(k.Value)
}
func (k IntKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case IntKey:
		return k.Value < t.Value
	case *IntKey:
		return k.Value < t.Value
	case *intKey:
		return k.Value < t.key.Value
	case FloatKey:
		return k.Value < int64(t.Value)
	case *FloatKey:
		return k.Value < int64(t.Value)
	case *floatKey:
		return k.Value < int64(t.key.Value)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, *stringKey:
		return true
	case nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k IntKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case IntKey:
		return k.Value < t.Value
	case *IntKey:
		return k.Value < t.Value
	case FloatKey:
		return k.Value < int64(t.Value)
	case *FloatKey:
		return k.Value < int64(t.Value)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey:
		return true
	case NilKey, *NilKey, nil:
		return false
	}
	return false
}
func (k IntKey) Compare(than btree.Item) int {
	switch t := than.(type) {
	case IntKey:
		if k.Value < t.Value {
			return -1
		} else if k.Value > t.Value {
			return 1
		} else {
			return 0
		}
	case *IntKey:
		if k.Value < t.Value {
			return -1
		} else if k.Value > t.Value {
			return 1
		} else {
			return 0
		}
	case *intKey:
		if k.Value < t.key.Value {
			return -1
		} else if k.Value > t.key.Value {
			return 1
		} else {
			return 0
		}
	case FloatKey:
		tv := int64(t.Value)
		if k.Value < tv {
			return -1
		} else if k.Value > tv {
			return 1
		} else {
			return 0
		}
	case *FloatKey:
		tv := int64(t.Value)
		if k.Value < tv {
			return -1
		} else if k.Value > tv {
			return 1
		} else {
			return 0
		}
	case *floatKey:
		tv := int64(t.key.Value)
		if k.Value < tv {
			return -1
		} else if k.Value > tv {
			return 1
		} else {
			return 0
		}
	case StringKey, *StringKey, *stringKey:
		return -1
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return 1
	}
	return 1
}
func (k IntKey) LessThanItem(than btree.Item, item *Item) bool {
	switch t := than.(type) {
	case IntKey:
		return k.Value < t.Value
	case *IntKey:
		return k.Value < t.Value
	case *intKey:
		if k.Value < t.key.Value {
			return true
		} else if k.Value > t.key.Value {
			return false
		} else {
			if item == nil {
				return t.item != nil
			} else if t.item == nil {
				return true
			} else {
				return item.Key < t.item.Key
			}
		}
	case FloatKey:
		return k.Value < int64(t.Value)
	case *FloatKey:
		return k.Value < int64(t.Value)
	case *floatKey:
		tv := int64(t.key.Value)
		if k.Value < tv {
			return true
		} else if k.Value > tv {
			return false
		} else {
			if item == nil {
				return t.item != nil
			} else if t.item == nil {
				return true
			} else {
				return item.Key < t.item.Key
			}
		}
	case StringKey, *StringKey, *stringKey:
		return true
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
}


type FloatKey struct {
	Value float64
}

func (k FloatKey) IsNil() bool {
	return false
}
func (k FloatKey) AsFloat() float64 {
	return k.Value
}
func (k FloatKey) AsInt64() int64 {
	return int64(k.Value)
}
func (k FloatKey) AsUInt64() uint64 {
	return uint64(k.Value)
}
func (k FloatKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case FloatKey:
		return k.Value < t.Value
	case *FloatKey:
		return k.Value < t.Value
	case *floatKey:
		return k.Value < t.key.Value
	case IntKey:
		return k.Value < float64(t.Value)
	case *IntKey:
		return k.Value < float64(t.Value)
	case *intKey:
		return k.Value < float64(t.key.Value)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, *stringKey:
		return true
	case nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k FloatKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case FloatKey:
		return k.Value < t.Value
	case *FloatKey:
		return k.Value < t.Value
	case IntKey:
		return k.Value < float64(t.Value)
	case *IntKey:
		return k.Value < float64(t.Value)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey:
		return true
	case NilKey, *NilKey, nil:
		return false
	}
	return false
}
func (k FloatKey) LessThanItem(than btree.Item, item *Item) bool {
	switch t := than.(type) {
	case FloatKey:
		return k.Value < t.Value
	case *FloatKey:
		return k.Value < t.Value
	case *floatKey:
		if k.Value < t.key.Value {
			return true
		} else if k.Value > t.key.Value {
			return false
		} else {
			if item == nil {
				return t.item != nil
			} else if t.item == nil {
				return true
			} else {
				return item.Key < t.item.Key
			}
		}
	case IntKey:
		return k.Value < float64(t.Value)
	case *IntKey:
		return k.Value < float64(t.Value)
	case *intKey:
		tv := float64(t.key.Value)
		if k.Value < tv {
			return true
		} else if k.Value > tv {
			return false
		} else {
			if item == nil {
				return t.item != nil
			} else if t.item == nil {
				return true
			} else {
				return item.Key < t.item.Key
			}
		}
	case StringKey, *StringKey, *stringKey:
		return true
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k FloatKey) Compare(than btree.Item) int {
	switch t := than.(type) {
	case FloatKey:
		if k.Value < t.Value {
			return -1
		} else if k.Value > t.Value {
			return 1
		} else {
			return 0
		}
	case *FloatKey:
		if k.Value < t.Value {
			return -1
		} else if k.Value > t.Value {
			return 1
		} else {
			return 0
		}
	case *floatKey:
		if k.Value < t.key.Value {
			return -1
		} else if k.Value > t.key.Value {
			return 1
		} else {
			return 0
		}
	case IntKey:
		tv := float64(t.Value)
		if k.Value < tv {
			return -1
		} else if k.Value > tv {
			return 1
		} else {
			return 0
		}
	case *IntKey:
		tv := float64(t.Value)
		if k.Value < tv {
			return -1
		} else if k.Value > tv {
			return 1
		} else {
			return 0
		}
	case *intKey:
		tv := float64(t.key.Value)
		if k.Value < tv {
			return -1
		} else if k.Value > tv {
			return 1
		} else {
			return 0
		}
	case StringKey, *StringKey, *stringKey:
		return -1
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return 1
	}
	return 1
}
