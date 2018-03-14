package sorted

import "github.com/pointc-io/ipdb/db/data/btree"

// A bunch of different combinations of composite indexes.
//
//
//
//

type CompositeItem struct {
	IndexItemBase
	Keys []Key
}

func (m *CompositeItem) Less(than btree.Item, ctx interface{}) bool {
	t := than.(*CompositeItem)
	tl := len(t.Keys)
	l := len(m.Keys)

	if tl == l {
		last := tl - 1
		for i := 0; i < tl; i++ {
			if i == last {
				return m.Keys[i].LessThan(t.Keys[i])
			} else {
				if m.Keys[i].LessThan(t.Keys[i]) {
					return true
				}
			}
		}

		return false
	} else if tl < l {
		last := tl - 1
		for i := 0; i < tl; i++ {
			if i == last {
				return m.Keys[i].LessThanItem(t.Keys[i], m.item)
			} else {
				if m.Keys[i].LessThan(t.Keys[i]) {
					return true
				}
			}
		}

		return false
	} else {
		last := l - 1
		for i := 0; i < tl; i++ {
			if i == last {
				return m.Keys[i].LessThan(t.Keys[i])
			} else {
				if m.Keys[i].LessThan(t.Keys[i]) {
					return true
				}
			}
		}

		return true
	}
}

type Composite2Item struct {
	IndexItemBase
	Key  Key
	Key2 Key
}

type Composite3Item struct {
	IndexItemBase
	Key  Key
	Key2 Key
	Key3 Key
}

type Composite4Item struct {
	IndexItemBase
	Key  Key
	Key2 Key
	Key3 Key
	Key4 Key
}

type Composite5Item struct {
	IndexItemBase
	Key  Key
	Key2 Key
	Key3 Key
	Key4 Key
	Key5 Key
}


//
//
//
//
//
//
//

type compositeKey2 struct {
	IndexItemBase
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
				return k.item.Key.LessThan(t.item.Key)
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
