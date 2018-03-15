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
	StringMax   = MaxStringKey{}
	MinKey      = NilKey{}
	MaxKey      = StringMax

	SkipKey    = NilKey{}
	Nil        = NilKey{}
	NotNil     = NilKey{}
	InvalidKey = NilKey{}
	True       = TrueKey{}
	False      = FalseKey{}
)

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
// to the most appropriate Key type.
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

func JSONToKey(result gjson.Result) Key {
	switch result.Type {
	// Null is a null json value
	case gjson.Null:
		return NilKey{}
		// False is a json false bool
	case gjson.False:
		return FalseKey{}
		// Number is json number
	case gjson.Number:
		return FloatKey(result.Num)
		// String is a json string
	case gjson.String:
		return StringKey(result.Str)
		// True is a json true bool
	case gjson.True:
		return TrueKey{}
		// JSON is a raw block of JSON
	case gjson.JSON:
		return StringKey(result.Raw)
	}
	return NilKey{}
}

type Key interface {
	btree.Item

	Type() sliced.DataType

	Match(pattern string) bool

	LessThan(key Key) bool

	LessThanItem(than btree.Item, item *ValueItem) bool
}

//
//
//
type NilKey struct {
}

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
	return true
}

//
//
//
type BoolKey struct{}

func (k BoolKey) Parse(arg []byte) Key {
	switch len(arg) {
	case 0:
		return Nil
	case 1:
		switch arg[0] {
		case 0x00:
			return False
		case 0x01:
			return True
		case '1', 'T', 't', 'Y', 'y':
			return True

		case '0', 'F', 'f', 'N', 'n':
			return False
		}
		return InvalidKey

	case 2:
		switch arg[0] {
		case 'N', 'n':
			switch arg[1] {
			case 'O', 'o':
				return False
			}
		}
		return InvalidKey

	case 3:
		switch arg[0] {
		case 'Y', 'y':
			switch arg[1] {
			case 'E', 'e':
				switch arg[2] {
				case 'S', 's':
					return True
				}
			}
		}
		return InvalidKey
	case 4:
		switch arg[0] {
		case 'T', 't':
			switch arg[1] {
			case 'R', 'r':
				switch arg[2] {
				case 'U', 'u':
					switch arg[3] {
					case 'E', 'e':
						return True
					}
				}
			}
		}
		return InvalidKey
	case 5:
		switch arg[0] {
		case 'F', 'f':
			switch arg[1] {
			case 'A', 'a':
				switch arg[2] {
				case 'L', 'l':
					switch arg[3] {
					case 'S', 's':
						switch arg[4] {
						case 'E', 'e':
							return False
						}
					}
				}
			}
		}
		return InvalidKey
	}
	return InvalidKey
}
func (k BoolKey) Type() sliced.DataType {
	return sliced.Bool
}
func (k BoolKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k BoolKey) Less(than btree.Item, ctx interface{}) bool {
	return true
}
func (k BoolKey) LessThan(than Key) bool {
	return true
}
func (k BoolKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	return true
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
	return true
}
func (k TrueKey) LessThan(than Key) bool {
	return true
}
func (k TrueKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	return true
}

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
	return true
}
func (k FalseKey) LessThan(than Key) bool {
	return true
}
func (k FalseKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	return true
}

//
//
//
type MaxStringKey struct{}

func (k MaxStringKey) Type() sliced.DataType {
	return sliced.String
}
func (k MaxStringKey) Match(pattern string) bool {
	return pattern == "*"
}
func (k MaxStringKey) Less(than btree.Item, ctx interface{}) bool {
	return false
}
func (k MaxStringKey) LessThan(than Key) bool {
	return false
}
func (k MaxStringKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	return false
}

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

//
func (k StringKey) Less(than btree.Item, ctx interface{}) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
	case StringKey:
		return (string)(k) < (string)(t)
	case *StringKey:
		return (string)(k) < (string)(*t)
	case *StringItem:
		return (string)(k) < (string)(t.Key)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case *StringCaseInsensitiveItem:
		return CaseInsensitiveCompare((string)(k), (string)(t.Key))
	case MaxStringKey, *MaxStringKey:
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
	case MaxStringKey, *MaxStringKey:
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
	case *StringItem:
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
	case *StringCaseInsensitiveItem:
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
	case MaxStringKey, *MaxStringKey:
		return true
	}
	return false
}

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
	case *StringItem:
		return CaseInsensitiveCompare((string)(k), (string)(t.Key))
	case *StringCaseInsensitiveItem:
		return CaseInsensitiveCompare((string)(k), (string)(t.Key))
	case MaxStringKey, *MaxStringKey:
		return true
	}
	return false
}
func (k StringCIKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
	case StringKey:
		return (string)(k) < (string)(t)
	case *StringKey:
		return (string)(k) < (string)(*t)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case MaxStringKey, *MaxStringKey:
		return true
	}
	return false
}
func (k StringCIKey) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
	case StringKey:
		return (string)(k) < (string)(t)
	case *StringKey:
		return (string)(k) < (string)(*t)
	case StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(t))
	case *StringCIKey:
		return CaseInsensitiveCompare((string)(k), (string)(*t))
	case *StringItem:
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

	case *StringCaseInsensitiveItem:
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
	case MaxStringKey, *MaxStringKey:
		return true
	}
	return false
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
	case *IntItem:
		return k < t.Key
	case FloatKey:
		return k < IntKey(t)
	case *FloatKey:
		return k < IntKey(*t)
	case *FloatItem:
		return k < IntKey(t.Key)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, *StringItem, MaxStringKey, *MaxStringKey:
		return true
	case nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k IntKey) LessThan(than Key) bool {
	switch t := than.(type) {
	case *ValueItem:
		return k.LessThan(t.Key)
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
	case StringKey, *StringKey, MaxStringKey, *MaxStringKey:
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
	case *IntItem:
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
	case *FloatItem:
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
	case StringKey, *StringKey, *StringItem, MaxStringKey, *MaxStringKey:
		return true
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
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
	case *FloatItem:
		return k < t.Key
	case IntKey:
		return k < FloatKey(t)
	case *IntKey:
		return k < FloatKey(*t)
	case *IntItem:
		return k < FloatKey(t.Key)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, *StringItem, MaxStringKey, *MaxStringKey:
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
	case *ValueItem:
		return k.LessThan(t.Key)
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
	case StringKey, *StringKey, MaxStringKey, *MaxStringKey:
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
	case *FloatItem:
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
		return k < FloatKey(t)
	case *IntKey:
		return k < FloatKey(*t)
	case *IntItem:
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
				return item.Key.LessThan(t.Key)
			}
		}
	case StringKey, *StringKey, *StringItem, MaxStringKey, *MaxStringKey:
		return true
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
}
func (k FloatKey) Compare(than btree.Item) int {
	switch t := than.(type) {
	case FloatKey:
		if k < t {
			return -1
		} else if k > t {
			return 1
		} else {
			return 0
		}
	case *FloatKey:
		if k < *t {
			return -1
		} else if k > *t {
			return 1
		} else {
			return 0
		}
	case *FloatItem:
		if k < t.Key {
			return -1
		} else if k > t.Key {
			return 1
		} else {
			return 0
		}
	case IntKey:
		tv := FloatKey(t)
		if k < tv {
			return -1
		} else if k > tv {
			return 1
		} else {
			return 0
		}
	case *IntKey:
		tv := FloatKey(*t)
		if k < tv {
			return -1
		} else if k > tv {
			return 1
		} else {
			return 0
		}
	case *IntItem:
		tv := FloatKey(t.Key)
		if k < tv {
			return -1
		} else if k > tv {
			return 1
		} else {
			return 0
		}
	case StringKey, *StringKey, *StringItem, MaxStringKey, *MaxStringKey:
		return -1
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return 1
	}
	return 1
}

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
	case Key2:
		if t._1.Less(t._1, ctx) {
			return true
		}
		return t._2.Less(t._2, ctx)
	default:
		return false
	}
	return false
}
func (k Key2) LessThan(than Key) bool {
	switch t := than.(type) {
	case NilKey, *NilKey, nil:
		return false
	case StringKey:
		return k._1.LessThan(t)
	case *StringKey:
		return k._1.LessThan(*t)
	case FloatKey:
		return k._1.LessThan(t)
	case *FloatKey:
		return k._1.LessThan(*t)
	case FloatDescKey:
		return k._1.LessThan(t)
	case *FloatDescKey:
		return k._1.LessThan(*t)
	case IntKey:
		return k._1.LessThan(t)
	case *IntKey:
		return k._1.LessThan(*t)
	case Key2:
		if k._1.LessThan(t._1) {
			return true
		}
		return t._2.LessThan(t._2)
	case *Key2:
		if k._1.LessThan(t._1) {
			return true
		}
		return t._2.LessThan(t._2)
		//case Key3:
		//	if k._1.LessThan(t._2) {
		//		return true
		//	}
		//	return t._2.LessThan(t._2)
		//case *Key3:
		//	if k._1.LessThan(t._2) {
		//		return true
		//	}
		//	return t._2.LessThan(t._2)
		//case Key4:
		//	if k._1.LessThan(t._2) {
		//		return true
		//	}
		//	return t._2.LessThan(t._2)
		//case *Key4:
		//	if k._1.LessThan(t._2) {
		//		return true
		//	}
		//	return t._2.LessThan(t._2)
	default:
		return false
	}
	return false
}
func (k Key2) LessThanItem(than btree.Item, item *ValueItem) bool {
	switch t := than.(type) {
	//case *Key2Item:
	//	return k.LessThan(t.Key)
	//case *Key3Item:
	//	return k.LessThan(t.Key)
	//case *Key4Item:
	//	return k.LessThan(t.Key)

	case Key:
		return k.LessThan(t)
	}
	return false
}

type Key3 struct {
	_1 Key
	_2 Key
	_3 Key
}

type Key4 struct {
	_1 Key
	_2 Key
	_3 Key
	_4 Key
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
	case *FloatItem:
		return k > FloatDescKey(t.Key)
	case IntKey:
		return k > FloatDescKey(t)
	case *IntKey:
		return k > FloatDescKey(*t)
	case *IntItem:
		return k > FloatDescKey(t.Key)
	case FalseKey, *FalseKey, TrueKey, *TrueKey:
		return false
	case StringKey, *StringKey, *StringItem, MaxStringKey, *MaxStringKey:
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
	case StringKey, *StringKey, MaxStringKey, *MaxStringKey:
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
	case *FloatItem:
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
	case *IntItem:
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
	case StringKey, *StringKey, *StringItem, MaxStringKey, *MaxStringKey:
		return true
	case FalseKey, *FalseKey, TrueKey, *TrueKey, nil, NilKey, *NilKey:
		return false
	}
	return false
}
