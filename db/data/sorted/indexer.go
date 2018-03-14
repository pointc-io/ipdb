package sorted

import (
	"github.com/pointc-io/ipdb/db/data"
	"github.com/pointc-io/ipdb/codec/gjson"
)

// Project as single key from a value
type KeyProjector func(item *Item) Key

type Indexer interface {
	Fields() int
	FieldAt(index int) *IndexField
	Index(index *SetIndex, item *Item) IndexItem
}

func ValueProjector(item *Item) Key {
	l := len(item.Value)
	if l == 0 {
		return StringMin
	}

	if item.Value[0] == '{' {
		return StringKey(item.Value)
	} else {
		return ParseRaw(item.Value)
	}
}

func JSONProjector(path string) KeyProjector {
	return func(item *Item) Key {
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

func JSONRectProjector(path string) KeyProjector {
	return func(item *Item) Key {
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

func JSONIndexer(path string, kind data.DataType, desc bool) *IndexField {
	return NewIndexField(path, kind, desc, JSONProjector(path))
}

func JSONSpatialIndexer(path string, kind data.DataType, desc bool) *IndexField {
	return NewIndexField(path, kind, desc, JSONRectProjector(path))
}

func NewIndexField(
	name string,
	kind data.DataType,
	desc bool,
	projector KeyProjector,
) *IndexField {
	f := &IndexField{
		name:      name,
		kind:      kind,
		desc:      desc,
		projector: projector,
	}

	switch kind {
	case data.Int:
		f.forceInt = true
		f.iint = true
		f.ifloat = true
		f.ibool = false
		f.inil = false
		f.istr = false

	case data.Float:
		f.forceInt = false
		f.iint = true
		f.ifloat = true
		f.ibool = false
		f.inil = false
		f.istr = false

	case data.String:
		f.forceInt = false
		f.iint = false
		f.ifloat = false
		f.ibool = false
		f.inil = true
		f.istr = true

	default:
		f.forceInt = false
		f.iint = true
		f.ifloat = true
		f.ibool = true
		f.inil = true
		f.istr = true
	}

	return f
}

type CompositeIndex struct {
	fields []*IndexField
}

func (i *CompositeIndex) Index(index *SetIndex, item *Item) IndexItem {
	l := len(i.fields)
	if l == 0 {
		return nil
	}
	switch len(i.fields) {
	case 0:
		return nil
	case 1:
		return i.fields[0].Index(index, item)
	case 2:
		key := i.fields[0].Key(index, item)
		if key == nil || key == SkipKey {
			return nil
		}
		key2 := i.fields[1].Key(index, item)
		if key2 == nil || key2 == SkipKey {
			return nil
		}
		return &Composite2Item{
			IndexItemBase: IndexItemBase{
				idx:  index,
				item: item,
			},
			Key:  key,
			Key2: key2,
		}
	case 3:
		key := i.fields[0].Key(index, item)
		if key == nil || key == SkipKey {
			return nil
		}
		key2 := i.fields[1].Key(index, item)
		if key2 == nil || key2 == SkipKey {
			return nil
		}
		key3 := i.fields[2].Key(index, item)
		if key3 == nil || key3 == SkipKey {
			return nil
		}
		return &Composite3Item{
			IndexItemBase: IndexItemBase{
				idx:  index,
				item: item,
			},
			Key:  key,
			Key2: key2,
			Key3: key3,
		}

	case 4:
		key := i.fields[0].Key(index, item)
		if key == nil || key == SkipKey {
			return nil
		}
		key2 := i.fields[1].Key(index, item)
		if key2 == nil || key2 == SkipKey {
			return nil
		}
		key3 := i.fields[2].Key(index, item)
		if key3 == nil || key3 == SkipKey {
			return nil
		}
		key4 := i.fields[3].Key(index, item)
		if key4 == nil || key4 == SkipKey {
			return nil
		}
		return &Composite4Item{
			IndexItemBase: IndexItemBase{
				idx:  index,
				item: item,
			},
			Key:  key,
			Key2: key2,
			Key3: key3,
			Key4: key4,
		}

	default:
		return nil
	}
}

// Meta data to describe the behavior of the index dimension
type IndexField struct {
	//btree bool
	//rtree bool
	//radtree bool
	name     string
	field    string
	length   int
	kind     data.DataType
	desc     bool
	inil     bool
	iint     bool
	ifloat   bool
	istr     bool
	ibool    bool
	forceInt bool

	projector KeyProjector
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

func (i *IndexField) Key(index *SetIndex, item *Item) Key {
	k := i.projector(item)
	if k == nil {
		if i.inil {
			return Nil
		} else {
			return SkipKey
		}
	}

	switch key := k.(type) {
	case IntKey:
		if !i.iint {
			return SkipKey
		}
	case FloatKey:
		if !i.ifloat {
			return nil
		} else {
			if i.forceInt {
				k = IntKey(key)
			}
		}
	case StringKey:
		if !i.istr {
			return SkipKey
		} else {
			// Truncate if necessary
			if i.length > 0 && len(key) > i.length {
				key = key[:i.length]
				k = key
			}
		}
	case NilKey:
		if i.inil {
			return k
		} else {
			return SkipKey
		}
	}

	return k
}

func (i *IndexField) Index(index *SetIndex, item *Item) IndexItem {
	val := i.projector(item)
	if val == SkipKey {
		return nil
	}

	switch key := val.(type) {
	default:
		return &AnyItem{
			IndexItemBase: IndexItemBase{
				idx:  index,
				item: item,
			},
			Key: key,
		}
	case IntKey:
		if !i.iint {
			return nil
		} else {
			return &IntItem{
				IndexItemBase: IndexItemBase{
					idx:  index,
					item: item,
				},
				Key: key,
			}
		}
	case FloatKey:
		if !i.ifloat {
			return nil
		} else {
			if i.forceInt {
				return &IntItem{
					IndexItemBase: IndexItemBase{
						idx:  index,
						item: item,
					},
					Key: IntKey(key),
				}
			} else {
				return &FloatItem{
					IndexItemBase: IndexItemBase{
						idx:  index,
						item: item,
					},
					Key: key,
				}
			}
		}
	case StringKey:
		if !i.istr {
			return nil
		} else {
			// Truncate if necessary
			if i.length > 0 && len(key) > i.length {
				key = key[:i.length]
			}
			return &StringItem{
				IndexItemBase: IndexItemBase{
					idx:  index,
					item: item,
				},
				Key: key,
			}
		}
	case NilKey:
		if i.inil {
			return &AnyItem{
				IndexItemBase: IndexItemBase{
					idx:  index,
					item: item,
				},
				Key: key,
			}
		} else {
			return nil
		}
	case Rect:
		return &RectItem{
			IndexItemBase: IndexItemBase{
				idx:  index,
				item: item,
			},
			Key: key,
		}
	}
}
