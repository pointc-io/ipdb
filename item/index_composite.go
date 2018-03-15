package item

import (
	"github.com/pointc-io/ipdb/index/btree"
)

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
				return m.Keys[i].LessThanItem(t.Keys[i], m.value)
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

//type Composite2Item struct {
//	IndexItemBase
//	Key  Key
//	Key2 Key
//}
//
//type Composite3Item struct {
//	IndexItemBase
//	Key  Key
//	Key2 Key
//	Key3 Key
//}
//
//type Composite4Item struct {
//	IndexItemBase
//	Key  Key
//	Key2 Key
//	Key3 Key
//	Key4 Key
//}
//
//type Composite5Item struct {
//	IndexItemBase
//	Key  Key
//	Key2 Key
//	Key3 Key
//	Key4 Key
//	Key5 Key
//}