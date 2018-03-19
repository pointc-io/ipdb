//
//
//
package item

import (
	"math"
	"strconv"
	"strings"
	"unsafe"

	"github.com/pointc-io/sliced"
	"github.com/pointc-io/sliced/codec/gjson"
	"github.com/pointc-io/sliced/index/btree"
	"github.com/pointc-io/sliced/match"
)

var (
	IntMinKey   = IntKey(-math.MaxInt64)
	IntMaxKey   = IntKey(math.MaxInt64)
	FloatMinKey = FloatKey(-math.MaxFloat64)
	FloatMaxKey = FloatKey(math.MaxFloat64)
	StringMin   = StringKey("")
	StringMax   = StringMaxKey{}
	MinKey      = NilKey{}
	MaxKey      = StringMax

	SkipKey    = NilKey{}
	Nil        = NilKey{}
	NotNil     = NilKey{}
	InvalidKey = NilKey{}
	True       = TrueKey{}
	False      = FalseKey{}
)

// Nil -> nilItem
// False -> falseItem
// True -> trueItem
// Int -> intItem
// IntDesc -> intDescItem
// Float -> floatItem
// FloatDesc -> floatDescItem
// String -> stringItem
// StringDesc -> stringDescItem
// StringCI -> stringCIItem
// StringCIDesc -> stringCIDescItem
type Key interface {
	// Satisfy the btree.Item interface so keys can be used directly as items
	// This removes the need to convert a key into it's Item representation and
	// thus saves an allocation.
	btree.Item

	// The data type being represented
	Type() sliced.DataType

	Match(pattern string) bool

	// Compare if current key is less than the key argument
	LessThan(key Key) bool

	// Compare if current key is less than btree item
	LessThanItem(than btree.Item, item *ValueItem) bool

	// Compare current key to key argument
	// -1 = Less Than
	//  0 = Equal
	//  1 = Greater Than
	Compare(key Key) int
}

//
func ParseKeyBytes(from []byte) Key {
	if from == nil {
		return NilKey{}
	}

	// Fast convert to string
	str := *(*string)(unsafe.Pointer(&from))

	return ParseKey(str)
}

// Parses an unknown key without any opts and converts
// to the most appropriate K type.
func ParseKey(str string) Key {
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
							return StringKey(str)
						}
					}

					// Try float.
					f, err := strconv.ParseFloat(str, 64)
					if err != nil {
						return StringKey(str)
					} else {
						return FloatKey(f)
					}
				default:
					return StringKey(str)
				}
			}
			// Try int64.
			f, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return StringKey(str)
			} else {
				return IntKey(f)
			}

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':

		case '.':
			for i = i + 1; i < l; i++ {
				switch str[i] {
				case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				default:
					return StringKey(str)
				}
			}

			// Try float.
			f, err := strconv.ParseFloat(str, 64)
			if err != nil {
				return StringKey(str)
			} else {
				return FloatKey(f)
			}
		default:
			return StringKey(str)
		}
	}
	// Try int64.
	f, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return StringKey(str)
	} else {
		return IntKey(f)
	}
}

// Parses with a String type hint
func ParseString(from []byte) Key {
	if from == nil {
		return NilKey{}
	} else {
		return StringKey(from)
	}
}

// Parses with a Float type hint
func ParseFloat(from []byte) Key {
	v, err := strconv.ParseFloat(string(from), 64)
	if err != nil {
		return NilKey{}
	} else {
		return FloatKey(v)
	}
}

// Parses with an Int type hint
func ParseInt(from []byte) Key {
	v, err := strconv.ParseInt(string(from), 10, 64)
	if err != nil {
		return NilKey{}
	} else {
		return IntKey(v)
	}
}

// Parses with a Date type hint
func ParseDate(from []byte) Key {
	return NilKey{}
}

// Converts a JSON value to the most appropriate key
func JSONToKey(result gjson.Result) Key {
	switch result.Type {
	// Null is a null json value
	case gjson.Null:
		return Nil
		// False is a json false bool
	case gjson.False:
		return False
		// Number is json number
	case gjson.Number:
		return FloatKey(result.Num)
		// String is a json string
	case gjson.String:
		return StringKey(result.Str)
		// True is a json true bool
	case gjson.True:
		return True
		// JSON is a raw block of JSON
	case gjson.JSON:
		return StringKey(result.Raw)
	}
	return Nil
}

//
//
//
type NilKey struct{}

func (k NilKey) Parse(arg []byte) Key {
	if len(arg) == 0 {
		return Nil
	} else {
		return NotNil
	}
}
func (k NilKey) Type() sliced.DataType {
	return sliced.Nil
}
func (k NilKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k NilKey) Less(than btree.Item, ctx interface{}) bool {
	return true
}
func (k NilKey) LessThan(than Key) bool {
	return true
}
func (k NilKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch to := than.(type) {
	case *nilItem:
		if item == nil {
			return true
		} else if to.value == nil {
			return false
		} else {
			return item.Key.LessThan(to.value.Key)
		}
	}
	return true
}
func (k NilKey) Compare(key Key) int {
	return -1
}

//
//
//
//type BoolKey struct{}
//
//func (k BoolKey) Parse(arg []byte) K {
//	switch len(arg) {
//	case 0:
//		return Nil
//	case 1:
//		switch arg[0] {
//		case 0x00:
//			return False
//		case 0x01:
//			return True
//		case '1', 'T', 't', 'Y', 'y':
//			return True
//
//		case '0', 'F', 'f', 'N', 'n':
//			return False
//		}
//		return InvalidKey
//
//	case 2:
//		switch arg[0] {
//		case 'N', 'n':
//			switch arg[1] {
//			case 'O', 'o':
//				return False
//			}
//		}
//		return InvalidKey
//
//	case 3:
//		switch arg[0] {
//		case 'Y', 'y':
//			switch arg[1] {
//			case 'E', 'e':
//				switch arg[2] {
//				case 'S', 's':
//					return True
//				}
//			}
//		}
//		return InvalidKey
//	case 4:
//		switch arg[0] {
//		case 'T', 't':
//			switch arg[1] {
//			case 'R', 'r':
//				switch arg[2] {
//				case 'U', 'u':
//					switch arg[3] {
//					case 'E', 'e':
//						return True
//					}
//				}
//			}
//		}
//		return InvalidKey
//	case 5:
//		switch arg[0] {
//		case 'F', 'f':
//			switch arg[1] {
//			case 'A', 'a':
//				switch arg[2] {
//				case 'L', 'l':
//					switch arg[3] {
//					case 'S', 's':
//						switch arg[4] {
//						case 'E', 'e':
//							return False
//						}
//					}
//				}
//			}
//		}
//		return InvalidKey
//	}
//	return InvalidKey
//}
//func (k BoolKey) Type() sliced.DataType {
//	return sliced.Bool
//}
//func (k BoolKey) Match(pattern string) bool {
//	return pattern == "*"
//}
//func (k BoolKey) Less(than btree.Item, ctx interface{}) bool {
//	return true
//}
//func (k BoolKey) LessThan(than K) bool {
//	return true
//}
//func (k BoolKey) LessThanItem(than btree.Item, item *ValueItem) bool {
//	return true
//}
//func (k BoolKey) Compare(key K) int {
//	return 1
//}

//
//
//
type FalseKey struct{}

func (k FalseKey) Type() sliced.DataType {
	return sliced.Bool
}
func (k FalseKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k FalseKey) Less(than btree.Item, ctx interface{}) bool {
	switch than.(type) {
	case NilKey, *NilKey, *nilItem:
		return false
	}
	return true
}
func (k FalseKey) LessThan(than Key) bool {
	switch than.(type) {
	case NilKey, *NilKey:
		return false
	}
	return true
}
func (k FalseKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch to := than.(type) {
	case NilKey, *NilKey, *nilItem:
		return false
	case *falseItem:
		if item == nil {
			return true
		} else if to.value == nil {
			return false
		} else {
			return item.Key.LessThan(to.value.Key)
		}
	}
	return true
}
func (k FalseKey) Compare(key Key) int {
	switch key.(type) {
	case FalseKey, *FalseKey:
		return 0
	case NilKey, *NilKey:
		return 1
	}
	return -1
}

//
//
//
type TrueKey struct{}

func (k TrueKey) Type() sliced.DataType {
	return sliced.Bool
}
func (k TrueKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k TrueKey) Less(than btree.Item, ctx interface{}) bool {
	switch than.(type) {
	case FalseKey, *FalseKey, *falseItem, NilKey, *NilKey, *nilItem:
		return false
	}
	return true
}
func (k TrueKey) LessThan(than Key) bool {
	switch than.(type) {
	case FalseKey, *FalseKey, NilKey, *NilKey:
		return false
	}
	return true
}
func (k TrueKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch to := than.(type) {
	case FalseKey, *FalseKey, *falseItem, NilKey, *NilKey, *nilItem:
		return false
	case *trueItem:
		if item == nil {
			return true
		} else if to.value == nil {
			return false
		} else {
			return item.Key.LessThan(to.value.Key)
		}
	}
	return true
}
func (k TrueKey) Compare(key Key) int {
	switch key.(type) {
	case TrueKey, *TrueKey:
		return 0
	case FalseKey, *FalseKey:
		return 1
	case NilKey:
		return 1
	}

	return -1
}

//
//
//
type TimeKey struct{}

//
//
//
type StringMaxKey struct{}

func (k StringMaxKey) Type() sliced.DataType {
	return sliced.String
}
func (k StringMaxKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k StringMaxKey) Less(than btree.Item, ctx interface{}) bool {
	return false
}
func (k StringMaxKey) LessThan(than Key) bool {
	return false
}
func (k StringMaxKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	return false
}
func (k StringMaxKey) Compare(key Key) int {
	return 1
}

//
//
//
type StringKey string

func (k StringKey) Parse(arg []byte) Key {
	if arg == nil {
		return Nil
	}
	return (StringKey)(*(*string)(unsafe.Pointer(&arg)))
}
func (k StringKey) Type() sliced.DataType {
	return sliced.String
}
func (k StringKey) Match(pattern string) bool {
	if pattern == "*" {
		return true
	} else {
		return match.Match((string)(k), pattern)
	}
}
func (k StringKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	// Handle Primary Keys
	case *ValueItem:
		return k.LessThan(t.Key)
	case StringKey:
		return (string)(k) < (string)(t)
	case *StringKey:
		return (string)(k) < (string)(*t)
	case *stringItem:
		return (string)(k) < (string)(t.K)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case *stringCIItem:
		return CaseInsensitiveCompare((string)(k), (string)(t.Key))
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case StringKey:
		return (string)(k) < (string)(t)
	case *StringKey:
		return (string)(k) < (string)(*t)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)

	case StringKey:
		return (string)(k) < (string)(t)
		//case *StringKey:
		//	return(string)(k) < (string)(t)
	case *stringItem:
		if (string)(k) < (string)(t.K) {
			return true
		} else if (string)(k) > (string)(t.K) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.K)
			}
		}
	case *stringCIItem:
		if (string)(k) < (string)(t.Key) {
			return true
		} else if (string)(k) > (string)(t.Key) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringKey) Compare(key Key) int {
	switch to := key.(type) {
	case StringKey:
		return strings.Compare((string)(k), (string)(to))
	case *StringKey:
		return strings.Compare((string)(k), (string)(*to))
	case StringCIKey:
		return strings.Compare((string)(k), (string)(to))
	case *StringCIKey:
		return strings.Compare((string)(k), (string)(*to))
	case StringMaxKey, *StringMaxKey:
		return -1
	}

	return -1
}

//
//
//
type StringDescKey string

func (k StringDescKey) Parse(arg []byte) Key {
	if arg == nil {
		return Nil
	}
	return (StringKey)(*(*string)(unsafe.Pointer(&arg)))
}
func (k StringDescKey) Type() sliced.DataType {
	return sliced.String
}
func (k StringDescKey) Match(pattern string) bool {
	if pattern == "*" {
		return true
	} else {
		return match.Match((string)(k), pattern)
	}
}
func (k StringDescKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	// Handle Primary Keys
	case *ValueItem:
		return k.LessThan(t.Key)
	case StringDescKey:
		return (string)(k) > (string)(t)
	case *StringDescKey:
		return (string)(k) > (string)(*t)
	case *stringDescItem:
		return (string)(k) > (string)(t.K)
	case StringKey:
		return (string)(k) > (string)(t)
	case *StringKey:
		return (string)(k) > (string)(*t)
	case *stringItem:
		return (string)(k) > (string)(t.K)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case *stringCIItem:
		return CaseInsensitiveCompare((string)(t.Key), (string)(k))
	case StringCIDescKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringCIDescKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case *stringCIDescItem:
		return CaseInsensitiveCompare((string)(t.Key), (string)(k))
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringDescKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case StringDescKey:
		return (string)(k) > (string)(t)
	case *StringDescKey:
		return (string)(k) > (string)(*t)
	case StringKey:
		return (string)(k) > (string)(t)
	case *StringKey:
		return (string)(k) > (string)(*t)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case StringCIDescKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringCIDescKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringDescKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)

	case StringDescKey:
		return (string)(k) > (string)(t)
	case *StringDescKey:
		return (string)(k) > (string)(*t)
	case *stringDescItem:
		if (string)(k) > (string)(t.K) {
			return true
		} else if (string)(k) < (string)(t.K) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.K)
			}
		}

	case StringKey:
		return (string)(k) > (string)(t)
	case *StringKey:
		return (string)(k) > (string)(*t)
	case *stringItem:
		if (string)(k) > (string)(t.K) {
			return true
		} else if (string)(k) < (string)(t.K) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.K)
			}
		}
	case StringCIKey:
		return (string)(k) > (string)(t)
	case *StringCIKey:
		return (string)(k) > (string)(*t)
	case *stringCIItem:
		if (string)(k) > (string)(t.Key) {
			return true
		} else if (string)(k) < (string)(t.Key) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case StringCIDescKey:
		return (string)(k) > (string)(t)
	case *StringCIDescKey:
		return (string)(k) > (string)(*t)
	case *stringCIDescItem:
		if (string)(k) > (string)(t.Key) {
			return true
		} else if (string)(k) < (string)(t.Key) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringDescKey) Compare(key Key) int {
	switch to := key.(type) {
	case StringDescKey:
		return strings.Compare((string)(to), (string)(k))
	case *StringDescKey:
		return strings.Compare((string)(*to), (string)(k))
	case StringKey:
		return strings.Compare((string)(to), (string)(k))
	case *StringKey:
		return strings.Compare((string)(*to), (string)(k))
	case StringCIKey:
		return strings.Compare((string)(to), (string)(k))
	case *StringCIKey:
		return strings.Compare((string)(*to), (string)(k))
	case StringMaxKey, *StringMaxKey:
		return -1
	}

	return -1
}

//
//
// Case Insensitive string
type StringCIKey string

func (k StringCIKey) Type() sliced.DataType {
	return sliced.String
}
func (k StringCIKey) Match(pattern string) bool {
	if pattern == "*" {
		return true
	}

	key := (string)(k)
	for i := 0; i < len(key); i++ {
		if key[i] >= 'A' && key[i] <= 'Z' {
			key = strings.ToLower(key)
			break
		}
	}
	return match.Match(key, pattern)
}
func (k StringCIKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case StringKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case *stringItem:
		return CaseInsensitiveCompare((string)(k), (string)(t.K))
	case *stringCIItem:
		return CaseInsensitiveCompare((string)(k), (string)(t.Key))
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringCIKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case StringKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case StringDescKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringDescKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case StringCIDescKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringCIDescKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringCIKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
	case StringKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case *stringItem:
		if (string)(k) < (string)(t.K) {
			return true
		} else if (string)(k) > (string)(t.K) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.K)
			}
		}

	case *stringCIItem:
		if CaseInsensitiveCompare((string)(k), (string)(t.Key)) {
			return true
		}

		if (string)(k) > (string)(t.Key) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringCIKey) Compare(key Key) int {
	switch to := key.(type) {
	case StringKey:
		return strings.Compare((string)(k), (string)(to))
	case *StringKey:
		return strings.Compare((string)(k), (string)(*to))
	case StringCIKey:
		return strings.Compare((string)(k), (string)(to))
	case *StringCIKey:
		return strings.Compare((string)(k), (string)(*to))
	case StringMaxKey, *StringMaxKey:
		return -1
	}

	return -1
}

//
//
// Case Insensitive string
type StringCIDescKey string

func (k StringCIDescKey) Type() sliced.DataType {
	return sliced.String
}
func (k StringCIDescKey) Match(pattern string) bool {
	if pattern == "*" {
		return true
	}

	key := (string)(k)
	for i := 0; i < len(key); i++ {
		if key[i] >= 'A' && key[i] <= 'Z' {
			key = strings.ToLower(key)
			break
		}
	}
	return match.Match(key, pattern)
}
func (k StringCIDescKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	// Handle Primary Keys
	case *ValueItem:
		return k.LessThan(t.Key)
	case StringDescKey:
		return (string)(k) > (string)(t)
	case *StringDescKey:
		return (string)(k) > (string)(*t)
	case *stringDescItem:
		return (string)(k) > (string)(t.K)
	case StringKey:
		return (string)(k) > (string)(t)
	case *StringKey:
		return (string)(k) > (string)(*t)
	case *stringItem:
		return (string)(k) > (string)(t.K)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case *stringCIItem:
		return CaseInsensitiveCompare((string)(t.Key), (string)(k))
	case StringCIDescKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringCIDescKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case *stringCIDescItem:
		return CaseInsensitiveCompare((string)(t.Key), (string)(k))
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringCIDescKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case StringDescKey:
		return (string)(k) > (string)(t)
	case *StringDescKey:
		return (string)(k) > (string)(*t)
	case StringKey:
		return (string)(k) > (string)(t)
	case *StringKey:
		return (string)(k) > (string)(*t)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case StringCIDescKey:
		return CaseInsensitiveCompare((string)(t), (string)(k))
	case *StringCIDescKey:
		return CaseInsensitiveCompare((string)(*t), (string)(k))
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringCIDescKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)

	case StringDescKey:
		return (string)(k) > (string)(t)
	case *StringDescKey:
		return (string)(k) > (string)(*t)
	case *stringDescItem:
		if (string)(k) > (string)(t.K) {
			return true
		} else if (string)(k) < (string)(t.K) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.K)
			}
		}

	case StringKey:
		return (string)(k) > (string)(t)
	case *StringKey:
		return (string)(k) > (string)(*t)
	case *stringItem:
		if (string)(k) > (string)(t.K) {
			return true
		} else if (string)(k) < (string)(t.K) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.K)
			}
		}
	case StringCIKey:
		return (string)(k) > (string)(t)
	case *StringCIKey:
		return (string)(k) > (string)(*t)
	case *stringCIItem:
		if (string)(k) > (string)(t.Key) {
			return true
		} else if (string)(k) < (string)(t.Key) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case StringCIDescKey:
		return (string)(k) > (string)(t)
	case *StringCIDescKey:
		return (string)(k) > (string)(*t)
	case *stringCIDescItem:
		if (string)(k) > (string)(t.Key) {
			return true
		} else if (string)(k) < (string)(t.Key) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case StringMaxKey, *StringMaxKey:
		return true
	}
	return false
}
func (k StringCIDescKey) Compare(key Key) int {
	switch to := key.(type) {
	case StringDescKey:
		return strings.Compare((string)(to), (string)(k))
	case *StringDescKey:
		return strings.Compare((string)(*to), (string)(k))
	case StringKey:
		return strings.Compare((string)(to), (string)(k))
	case *StringKey:
		return strings.Compare((string)(*to), (string)(k))
	case StringCIKey:
		return strings.Compare((string)(to), (string)(k))
	case *StringCIKey:
		return strings.Compare((string)(*to), (string)(k))
	case StringMaxKey, *StringMaxKey:
		return -1
	}

	return -1
}

//func CaseInsensitiveCompare(a, b StringKey) bool {
//	for i := 0; i < len(a) && i < len(b); i++ {
//		if a[i] >= 'A' && a[i] <= 'Z' {
//			if b[i] >= 'A' && b[i] <= 'Z' {
//				// both are uppercase, do nothing
//				if a[i] < b[i] {
//					return true
//				} else if a[i] > b[i] {
//					return false
//				}
//			} else {
//				// a is uppercase, convert a to lowercase
//				if a[i]+32 < b[i] {
//					return true
//				} else if a[i]+32 > b[i] {
//					return false
//				}
//			}
//		} else if b[i] >= 'A' && b[i] <= 'Z' {
//			// b is uppercase, convert b to lowercase
//			if a[i] < b[i]+32 {
//				return true
//			} else if a[i] > b[i]+32 {
//				return false
//			}
//		} else {
//			// neither are uppercase
//			if a[i] < b[i] {
//				return true
//			} else if a[i] > b[i] {
//				return false
//			}
//		}
//	}
//	return len(a) < len(b)
//}

//
//
//
type IntKey int64

func (k IntKey) String() string {
	return strconv.Itoa(int(k))
}
func (k IntKey) Type() sliced.DataType {
	return sliced.Int
}
func (k IntKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k IntKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
	case IntKey:
		return k < t
	case *IntKey:
		return k < *t
	case *intItem:
		return k < t.Key
	case FloatKey:
		return k < IntKey(t)
	case *FloatKey:
		return k < IntKey(*t)
	case *floatItem:
		return k < IntKey(t.Key)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, *stringItem, StringMaxKey, *StringMaxKey:
		return true
	case nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k IntKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case IntKey:
		return k < t
	case *IntKey:
		return k < *t
	case FloatKey:
		return k < IntKey(t)
	case *FloatKey:
		return k < IntKey(*t)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, StringMaxKey, *StringMaxKey:
		return true
	case NilKey, *NilKey, nil:
		return false
	}
	return false
}
func (k IntKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
	case IntKey:
		return k < t
	case *IntKey:
		return k < *t
	case *intItem:
		if k < t.Key {
			return true
		} else if k > t.Key {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case FloatKey:
		return k < IntKey(t)
	case *FloatKey:
		return k < IntKey(*t)
	case *floatItem:
		tv := IntKey(t.Key)
		if k < tv {
			return true
		} else if k > tv {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case StringKey, *StringKey, *stringItem, StringMaxKey, *StringMaxKey:
		return true
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k IntKey) Compare(key Key) int {
	switch to := key.(type) {
	case IntKey:
		if k < to {
			return -1
		} else if k > to {
			return 1
		} else {
			return 0
		}
	case *IntKey:
		if k < *to {
			return -1
		} else if k > *to {
			return 1
		} else {
			return 0
		}
	case FloatKey:
		tv := IntKey(to)
		if k < tv {
			return -1
		} else if k > tv {
			return 1
		} else {
			return 0
		}
	case *FloatKey:
		tv := IntKey(*to)
		if k < tv {
			return -1
		} else if k > tv {
			return 1
		} else {
			return 0
		}
	case FloatDescKey:
		if k > (IntKey)(to) {
			return -1
		} else if k < (IntKey)(to) {
			return 1
		} else {
			return 0
		}
	case *FloatDescKey:
		if k > (IntKey)(*to) {
			return -1
		} else if k < (IntKey)(*to) {
			return 1
		} else {
			return 0
		}
	case NilKey, *NilKey, FalseKey, *FalseKey, TrueKey, *TrueKey:
		return 1
	case StringKey, *StringKey, StringCIKey, *StringCIKey, StringMaxKey, *StringMaxKey:
		return -1
	}
	return 1
}

//
//
//
type IntDescKey int64

func (k IntDescKey) String() string {
	return strconv.Itoa(int(k))
}
func (k IntDescKey) Type() sliced.DataType {
	return sliced.Int
}
func (k IntDescKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k IntDescKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
	case IntKey:
		return k > IntDescKey(t)
	case *IntKey:
		return k > IntDescKey(*t)
	case *intItem:
		return k > IntDescKey(t.Key)
	case FloatKey:
		return k > IntDescKey(t)
	case *FloatKey:
		return k > IntDescKey(*t)
	case FloatDescKey:
		return k > IntDescKey(t)
	case *FloatDescKey:
		return k > IntDescKey(*t)
	case *floatItem:
		return k > (IntDescKey)(t.Key)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, *stringItem, StringMaxKey, *StringMaxKey:
		return true
	case nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k IntDescKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case IntDescKey:
		return k > t
	case *IntDescKey:
		return k > *t
	case IntKey:
		return k > IntDescKey(t)
	case *IntKey:
		return k > IntDescKey(*t)
	case FloatKey:
		return k > IntDescKey(t)
	case *FloatKey:
		return k > IntDescKey(*t)
	case FloatDescKey:
		return k > IntDescKey(t)
	case *FloatDescKey:
		return k > IntDescKey(*t)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, StringMaxKey, *StringMaxKey:
		return true
	case NilKey, *NilKey, nil:
		return false
	}
	return false
}
func (k IntDescKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
	case IntDescKey:
		return k > t
	case *IntDescKey:
		return k > *t
	case *intDescItem:
		if k < t.Key {
			return true
		} else if k > t.Key {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case IntKey:
		return k > IntDescKey(t)
	case *IntKey:
		return k > IntDescKey(*t)
	case *intItem:
		tk := IntDescKey(t.Key)
		if k < tk {
			return true
		} else if k > tk {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case FloatKey:
		return k < IntDescKey(t)
	case *FloatKey:
		return k < IntDescKey(*t)
	case *floatItem:
		tk := IntDescKey(t.Key)
		if k < tk {
			return true
		} else if k > tk {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case StringKey, *StringKey, *stringItem, StringMaxKey, *StringMaxKey:
		return true
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k IntDescKey) Compare(key Key) int {
	switch to := key.(type) {
	case IntDescKey:
		if k > to {
			return -1
		} else if k < IntDescKey(to) {
			return 1
		} else {
			return 0
		}
	case *IntDescKey:
		if k > *to {
			return -1
		} else if k < *to {
			return 1
		} else {
			return 0
		}
	case IntKey:
		if k > IntDescKey(to) {
			return -1
		} else if k < IntDescKey(to) {
			return 1
		} else {
			return 0
		}
	case *IntKey:
		if k > IntDescKey(*to) {
			return -1
		} else if k < IntDescKey(*to) {
			return 1
		} else {
			return 0
		}
	case FloatKey:
		tv := IntDescKey(to)
		if k > tv {
			return -1
		} else if k < tv {
			return 1
		} else {
			return 0
		}
	case *FloatKey:
		tv := IntDescKey(*to)
		if k > tv {
			return -1
		} else if k < tv {
			return 1
		} else {
			return 0
		}
	case FloatDescKey:
		if k > IntDescKey(to) {
			return -1
		} else if k < IntDescKey(to) {
			return 1
		} else {
			return 0
		}
	case *FloatDescKey:
		if k > IntDescKey(*to) {
			return -1
		} else if k < IntDescKey(*to) {
			return 1
		} else {
			return 0
		}
	case NilKey, *NilKey, FalseKey, *FalseKey, TrueKey, *TrueKey:
		return 1
	case StringKey, *StringKey, StringCIKey, *StringCIKey, StringMaxKey, *StringMaxKey:
		return -1
	}
	return 1
}

//
//
//
type FloatKey float64

func (k FloatKey) Type() sliced.DataType {
	return sliced.Float
}
func (k FloatKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k FloatKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case FloatDescKey:
		return k > FloatKey(t)
	case *FloatDescKey:
		return k > FloatKey(*t)
	case *ValueItem:
		return k.LessThan(t.Key)
	case FloatKey:
		return k < t
	case *FloatKey:
		return k < *t
	case *floatItem:
		return k < t.Key
	case IntKey:
		return k < FloatKey(t)
	case *IntKey:
		return k < FloatKey(*t)
	case *intItem:
		return k < FloatKey(t.Key)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, *stringItem, StringMaxKey, *StringMaxKey:
		return true
	case nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k FloatKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case FloatDescKey:
		return k > FloatKey(t)
	case *FloatDescKey:
		return k > FloatKey(*t)
	case FloatKey:
		return k < t
	case *FloatKey:
		return k < *t
	case IntKey:
		return k < FloatKey(t)
	case *IntKey:
		return k < FloatKey(*t)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, StringMaxKey, *StringMaxKey:
		return true
	case NilKey, *NilKey, nil:
		return false
	}
	return false
}
func (k FloatKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	case FloatDescKey:
		return k > FloatKey(t)
	case *FloatDescKey:
		return k > FloatKey(*t)
	case *ValueItem:
		return k.LessThan(t.Key)
	case FloatKey:
		return k < t
	case *FloatKey:
		return k < *t
	case *floatItem:
		if k < t.Key {
			return true
		} else if k > t.Key {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.value.Key)
			}
		}
	case IntKey:
		return k < FloatKey(t)
	case *IntKey:
		return k < FloatKey(*t)
	case *intItem:
		tv := FloatKey(t.Key)
		if k < tv {
			return true
		} else if k > tv {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.value.Key)
			}
		}
	case StringKey, *StringKey, *stringItem, StringMaxKey, *StringMaxKey:
		return true
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k FloatKey) Compare(key Key) int {
	switch to := key.(type) {
	case FloatKey:
		if k < to {
			return -1
		} else if k > to {
			return 1
		} else {
			return 0
		}
	case *FloatKey:
		if k < *to {
			return -1
		} else if k > *to {
			return 1
		} else {
			return 0
		}
	case FloatDescKey:
		if k > (FloatKey)(to) {
			return -1
		} else if k < (FloatKey)(to) {
			return 1
		} else {
			return 0
		}
	case *FloatDescKey:
		if k > (FloatKey)(*to) {
			return -1
		} else if k < (FloatKey)(*to) {
			return 1
		} else {
			return 0
		}
	case IntKey:
		tv := FloatKey(to)
		if k < tv {
			return -1
		} else if k > tv {
			return 1
		} else {
			return 0
		}
	case *IntKey:
		tv := FloatKey(*to)
		if k < tv {
			return -1
		} else if k > tv {
			return 1
		} else {
			return 0
		}
	case NilKey, *NilKey, FalseKey, *FalseKey, TrueKey, *TrueKey:
		return 1
	case StringKey, *StringKey, StringCIKey, *StringCIKey, StringMaxKey, *StringMaxKey:
		return -1
	}
	return 1
}

//
//
//
type FloatDescKey float64

func (k FloatDescKey) Type() sliced.DataType {
	return sliced.Float
}
func (k FloatDescKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k FloatDescKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case FloatKey:
		return k > FloatDescKey(t)
	case *FloatKey:
		return k > FloatDescKey(*t)
	case *floatItem:
		return k > FloatDescKey(t.Key)
	case IntKey:
		return k > FloatDescKey(t)
	case *IntKey:
		return k > FloatDescKey(*t)
	case *intItem:
		return k > FloatDescKey(t.Key)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, *stringItem, StringMaxKey, *StringMaxKey:
		return true
	case nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k FloatDescKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case FloatDescKey:
		return k > FloatDescKey(t)
	case *FloatDescKey:
		return k > FloatDescKey(*t)
	case FloatKey:
		return k > FloatDescKey(t)
	case *FloatKey:
		return k > FloatDescKey(*t)
	case IntKey:
		return k > FloatDescKey(t)
	case *IntKey:
		return k > FloatDescKey(*t)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, StringMaxKey, *StringMaxKey:
		return true
	case NilKey, *NilKey, nil:
		return false
	}
	return false
}
func (k FloatDescKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	case FloatDescKey:
		return k > t
	case *FloatDescKey:
		return k > *t
	case FloatKey:
		return k > FloatDescKey(t)
	case *FloatKey:
		return k > FloatDescKey(*t)
	case *floatItem:
		if k > FloatDescKey(t.Key) {
			return true
		} else if k < FloatDescKey(t.Key) {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case IntKey:
		return k > FloatDescKey(t)
	case *IntKey:
		return k > FloatDescKey(*t)
	case *intItem:
		tv := FloatDescKey(t.Key)
		if k > tv {
			return true
		} else if k < tv {
			return false
		} else {
			if item == nil {
				return t.value != nil
			} else if t.value == nil {
				return true
			} else {
				return item.Key.LessThan(t.Key)
			}
		}
	case StringKey, *StringKey, *stringItem, StringMaxKey, *StringMaxKey:
		return true
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k FloatDescKey) Compare(key Key) int {
	switch to := key.(type) {
	case FloatKey:
		if k > (FloatDescKey)(to) {
			return -1
		} else if k > (FloatDescKey)(to) {
			return 1
		} else {
			return 0
		}
	case *FloatKey:
		if k < (FloatDescKey)(*to) {
			return -1
		} else if k > (FloatDescKey)(*to) {
			return 1
		} else {
			return 0
		}
	case FloatDescKey:
		if k > to {
			return -1
		} else if k < to {
			return 1
		} else {
			return 0
		}
	case *FloatDescKey:
		if k > *to {
			return -1
		} else if k < *to {
			return 1
		} else {
			return 0
		}
	case IntKey:
		tv := FloatDescKey(to)
		if k > tv {
			return -1
		} else if k < tv {
			return 1
		} else {
			return 0
		}
	case *IntKey:
		tv := FloatDescKey(*to)
		if k > tv {
			return -1
		} else if k > tv {
			return 1
		} else {
			return 0
		}
	case NilKey, *NilKey, FalseKey, *FalseKey, TrueKey, *TrueKey:
		return 1
	case StringKey, *StringKey, StringCIKey, *StringCIKey, StringMaxKey, *StringMaxKey:
		return -1
	}
	return 1
}

// Composite keys have 2 or more keys and the keys cannot be other composite keys
//
// Composite K with 2 Elements
type Key2 struct {
	_1 Key
	_2 Key
}

func (k Key2) Type() sliced.DataType {
	return sliced.Any
}
func (k Key2) Match(pattern string) bool {
	return k._1.Match(pattern) && k._2.Match(pattern)
}
func (k Key2) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case *AnyItem:
		return false
	case *composite2Item:
		switch k._1.Compare(t.K._1) {
		case -1:
			return true
		case 1:
			return false
		}
		switch k._2.Compare(t.K._2) {
		case -1:
			return true
		case 1:
			return false
		default:
			return false
		}
	case Key2:
		if k._1.LessThan(t._1) {
			return true
		}
		return k._2.LessThan(t._2)
	case *Key2:
		if k._1.LessThan(t._1) {
			return true
		}
		return k._2.LessThan(t._2)
		//case Key3:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case *Key3:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case Key4:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case *Key4:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case Key5:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case *Key5:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
	default:
		return false
	}
	return false
}
func (k Key2) LessThan(than Key) bool {
	switch t := than.(type) {
	case Key2:
		if k._1.LessThan(t._1) {
			return true
		}
		return k._2.LessThan(t._2)
	case *Key2:
		if k._1.LessThan(t._1) {
			return true
		}
		return k._2.LessThan(t._2)
		//case Key3:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case *Key3:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case Key4:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case *Key4:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case Key5:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
		//case *Key5:
		//	if k._1.LessThan(t._1) {
		//		return true
		//	}
		//	return k._2.LessThan(t._2)
	default:
		return false
	}
}
func (k Key2) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch to := than.(type) {
	case *composite2Item:
		switch k._1.Compare(to.K._1) {
		case -1:
			return true
		case 1:
			return false
		}
		switch k._2.Compare(to.K._2) {
		case -1:
			return true
		case 1:
			return false
		default:
			if item == nil {
				return to.value != nil
			} else if to.value == nil {
				return true
			} else {
				return item.Key.LessThan(to.value.Key)
			}
		}
	case Key2:
		if k._1.LessThan(to._1) {
			return true
		}
		return k._2.LessThan(to._2)
	case *Key2:
		if k._1.LessThan(to._1) {
			return true
		}
		return k._2.LessThan(to._2)
		//case Key3:
		//	if k._1.LessThan(to._1) {
		//		return true
		//	}
		//	return k._2.LessThan(to._2)
		//case *Key3:
		//	if k._1.LessThan(to._1) {
		//		return true
		//	}
		//	return k._2.LessThan(to._2)
		//case Key4:
		//	if k._1.LessThan(to._1) {
		//		return true
		//	}
		//	return k._2.LessThan(to._2)
		//case *Key4:
		//	if k._1.LessThan(to._1) {
		//		return true
		//	}
		//	return k._2.LessThan(to._2)
		//case Key5:
		//	if k._1.LessThan(to._1) {
		//		return true
		//	}
		//	return k._2.LessThan(to._2)
		//case *Key5:
		//	if k._1.LessThan(to._1) {
		//		return true
		//	}
		//	return k._2.LessThan(to._2)

	case *nilItem:
		return false
	case *trueItem:
		return k._1.LessThan(True)
	case *falseItem:
		return k._1.LessThan(False)
	case *intItem:
		return k._1.LessThan(to.Key)
	case *intDescItem:
		return k._1.LessThan(to.Key)
	case *floatItem:
		return k._1.LessThan(to.Key)
	case *floatDescItem:
		return k._1.LessThan(to.Key)
	case *stringItem:
		return k._1.LessThan(to.K)
	case *stringDescItem:
		return k._1.LessThan(to.K)
	case *stringCIItem:
		return k._1.LessThan(to.Key)
	case *stringCIDescItem:
		return k._1.LessThan(to.Key)
	}
	return false
}
func (k Key2) Compare(key Key) int {
	return 1
}

//
//
//
type Key3 struct {
	_1 Key
	_2 Key
	_3 Key
}

//
//
//
type Key4 struct {
	_1 Key
	_2 Key
	_3 Key
	_4 Key
}

//
//
//
type Key5 struct {
	_1 Key
	_2 Key
	_3 Key
	_4 Key
	_5 Key
}
