package item

import (
	"github.com/pointc-io/ipdb/codec/gjson"
)

type IndexOpts int

const (
	IncludeString   IndexOpts = 1 << iota
	IncludeInt
	IncludeFloat
	IncludeBool
	IncludeRect
	IncludeNil
	IncludeAny
	IndexFloatAsInt
	SortDesc
)

// Project as single key from a value
type KeyProjector func(item *ValueItem) Key

//
type Indexer interface {
	// Parses raw RESP args
	ParseArgs(offset int, buf [][]byte) Key
	// Parse a single RESP arg
	ParseArg(arg []byte) Key
	// Number of fields in index
	Fields() int
	// Retrieve an IndexField at a position
	FieldAt(pos int) *IndexField
	// Factory function
	Index(index *Index, item *ValueItem) IndexItem
}

//
//
//
func ValueProjector(item *ValueItem) Key {
	l := len(item.Value)
	if l == 0 {
		return StringMin
	}

	if item.Value[0] == '{' {
		return StringKey(item.Value)
	} else {
		return ParseKey(item.Value)
	}
}

//
//
//
func RectProjector(item *ValueItem) Key {
	return ParseRect(item.Value)
}

//
//
//
func JSONProjector(path string) KeyProjector {
	return func(item *ValueItem) Key {
		result := gjson.Get(item.Value, path)
		switch result.Type {
		case gjson.String:
			return StringKey(result.Str)
		case gjson.Null:
			return Nil
		case gjson.JSON:
			return NilKey{}
		case gjson.True:
			return TrueKey{}
		case gjson.False:
			return FalseKey{}
		case gjson.Number:
			return FloatKey(result.Num)
		default:
			return StringKey(result.Raw)
		}
	}
}

//
//
//
func JSONRectProjector(path string) KeyProjector {
	return func(item *ValueItem) Key {
		result := gjson.Get(item.Value, path)

		var rect Rect
		if result.Type == gjson.String {
			rect = ParseRect(result.Str)
		} else if result.Type == gjson.JSON {
			if result.Raw[0] == '[' {
				rect = ParseRect(result.Raw)
			} else {
				return SkipKey
			}
		} else {
			return SkipKey
		}

		if rect.Min == nil && rect.Max == nil {
			return SkipKey
		}
		return rect
	}
}

//
//
//
func JSONIndexer(path string, opts IndexOpts) *IndexField {
	return NewIndexer(path, opts, JSONProjector(path))
}

//
//
//
func IndexString(desc bool) IndexOpts {
	o := IncludeString
	if desc {
		o |= SortDesc
	}
	return o
}

//
//
//
func IndexInt(desc bool) IndexOpts {
	o := IncludeInt | IncludeFloat | IndexFloatAsInt
	if desc {
		o |= SortDesc
	}
	return o
}

//
//
//
func IndexFloat(desc bool) IndexOpts {
	o := IncludeInt | IncludeFloat
	if desc {
		o |= SortDesc
	}
	return o
}

//
//
//
func IndexAny(desc bool) IndexOpts {
	o := IncludeInt | IncludeFloat | IncludeString | IncludeNil
	if desc {
		o |= SortDesc
	}
	return o
}

//
//
//
func IndexSpatial() IndexOpts {
	return IncludeRect
}

//
//
//
func SpatialIndexer() *IndexField {
	return NewIndexer("", IndexSpatial(), RectProjector)
}

//
//
//
func JSONSpatialIndexer(path string) *IndexField {
	return NewIndexer(path, IndexSpatial(), JSONRectProjector(path))
}

//
//
//
func NewIndexer(
	name string,
	opts IndexOpts,
	projector KeyProjector,
) *IndexField {
	return &IndexField{
		name:      name,
		opts:      opts,
		projector: projector,
	}
}

//type CompositeIndex struct {
//	fields []*IndexField
//}
//
//func (i *CompositeIndex) Index(index *Index, item *ValueItem) IndexItem {
//	l := len(i.fields)
//	if l == 0 {
//		return nil
//	}
//	switch len(i.fields) {
//	case 0:
//		return nil
//	case 1:
//		return i.fields[0].Index(index, item)
//	case 2:
//		key := i.fields[0].Key(index, item)
//		if key == nil || key == SkipKey {
//			return nil
//		}
//		key2 := i.fields[1].Key(index, item)
//		if key2 == nil || key2 == SkipKey {
//			return nil
//		}
//		return &Composite2Item{
//			IndexItemBase: IndexItemBase{
//				idx:   index,
//				value: item,
//			},
//			Key:  key,
//			Key2: key2,
//		}
//	case 3:
//		key := i.fields[0].Key(index, item)
//		if key == nil || key == SkipKey {
//			return nil
//		}
//		key2 := i.fields[1].Key(index, item)
//		if key2 == nil || key2 == SkipKey {
//			return nil
//		}
//		key3 := i.fields[2].Key(index, item)
//		if key3 == nil || key3 == SkipKey {
//			return nil
//		}
//		return &Composite3Item{
//			IndexItemBase: IndexItemBase{
//				idx:   index,
//				value: item,
//			},
//			Key:  key,
//			Key2: key2,
//			Key3: key3,
//		}
//
//	case 4:
//		key := i.fields[0].Key(index, item)
//		if key == nil || key == SkipKey {
//			return nil
//		}
//		key2 := i.fields[1].Key(index, item)
//		if key2 == nil || key2 == SkipKey {
//			return nil
//		}
//		key3 := i.fields[2].Key(index, item)
//		if key3 == nil || key3 == SkipKey {
//			return nil
//		}
//		key4 := i.fields[3].Key(index, item)
//		if key4 == nil || key4 == SkipKey {
//			return nil
//		}
//		return &Composite4Item{
//			IndexItemBase: IndexItemBase{
//				idx:   index,
//				value: item,
//			},
//			Key:  key,
//			Key2: key2,
//			Key3: key3,
//			Key4: key4,
//		}
//
//	default:
//		return nil
//	}
//}

// Meta data to describe the behavior of the index dimension
type IndexField struct {
	name      string
	length    int
	opts      IndexOpts
	projector KeyProjector
}

func (i *IndexField) ParseArgs(offset int, buf [][]byte) Key {
	if len(buf) < offset {
		return Nil
	}
	return i.Parse(buf[offset])
}

func (i *IndexField) Parse(buf []byte) Key {
	switch key := ParseKeyBytes(buf).(type) {
	case IntKey:
		if i.opts&IncludeInt != 0 {
			return key
		} else {
			return SkipKey
		}
	case StringKey:
		if i.opts&IncludeString != 0 {
			return key
		} else {
			return SkipKey
		}
	case FloatKey:
		if i.opts&IncludeFloat != 0 {
			return key
		} else {
			return SkipKey
		}
	default:
		return key
	}
}

func (i *IndexField) Fields() int {
	return 1
}

func (i *IndexField) FieldAt(index int) *IndexField {
	if index == 0 {
		return i
	} else {
		return nil
	}
}

func (i *IndexField) Key(index *Index, item *ValueItem) Key {
	k := i.projector(item)
	if k == nil {
		return SkipKey
	}

	switch key := k.(type) {
	case IntKey:
		if i.opts&IncludeInt != 0 {
			return key
		} else {
			return SkipKey
		}
	case FloatKey:
		if i.opts&IncludeFloat != 0 {
			if i.opts&IndexFloatAsInt != 0 {
				return IntKey(key)
			} else {
				return key
			}
		} else {
			return SkipKey
		}
	case StringKey:
		if i.opts&IncludeString != 0 {
			// Truncate if necessary
			if i.length > 0 && len(key) > i.length {
				key = key[:i.length]
				k = key
			}
			return k
		} else {
			return SkipKey
		}
	case NilKey:
		if i.opts&IncludeNil != 0 {
			return Nil
		} else {
			return SkipKey
		}
	}

	return k
}

func (i *IndexField) Index(index *Index, item *ValueItem) IndexItem {
	// Project a key from the value
	val := i.projector(item)

	// Should we skip?
	if val == SkipKey {
		return nil
	}

	// The general logic is duplicated with "Key()"
	// Only done to save a type assertion for the most
	// likely path of a single field index.
	switch key := val.(type) {
	default:
		return &AnyItem{
			IndexItemBase: IndexItemBase{
				idx:   index,
				value: item,
			},
			Key: key,
		}
	case IntKey:
		if i.opts&IncludeInt != 0 {
			return &IntItem{
				IndexItemBase: IndexItemBase{
					idx:   index,
					value: item,
				},
				Key: key,
			}
		} else {
			return nil
		}
	case FloatKey:
		if i.opts&IncludeFloat != 0 {
			if i.opts&IndexFloatAsInt != 0 {
				return &IntItem{
					IndexItemBase: IndexItemBase{
						idx:   index,
						value: item,
					},
					Key: IntKey(key),
				}
			} else {
				if i.opts&SortDesc != 0 {
					return &FloatDescItem{
						IndexItemBase: IndexItemBase{
							idx:   index,
							value: item,
						},
						Key: FloatDescKey(key),
					}
				}
				return &FloatItem{
					IndexItemBase: IndexItemBase{
						idx:   index,
						value: item,
					},
					Key: key,
				}
			}
		} else {
			return nil
		}
	case StringKey:
		if i.opts&IncludeString != 0 {
			// Truncate if necessary
			if i.length > 0 && len(key) > i.length {
				key = key[:i.length]
			}
			return &StringItem{
				IndexItemBase: IndexItemBase{
					idx:   index,
					value: item,
				},
				Key: key,
			}
		} else {
			return nil
		}

	case NilKey:
		if i.opts&IncludeNil != 0 {
			return &AnyItem{
				IndexItemBase: IndexItemBase{
					idx:   index,
					value: item,
				},
				Key: key,
			}
		} else {
			return nil
		}
	case Rect:
		if i.opts&IncludeRect != 0 {
			return &RectItem{
				IndexItemBase: IndexItemBase{
					idx:   index,
					value: item,
				},
				Key: key,
			}
		} else {
			return nil
		}
	}
}
