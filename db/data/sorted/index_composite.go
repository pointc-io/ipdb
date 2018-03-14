package sorted

import "github.com/pointc-io/ipdb/db/data/btree"

// A bunch of different combinations of composite indexes.
//
//
//
//

type multiIndexItem struct {
	indexItem
	keys []Key
}

func (m *multiIndexItem) Less(than btree.Item, ctx interface{}) bool {
	t := than.(*multiIndexItem)
	tl := len(t.keys)
	l := len(m.keys)

	if tl == l {
		last := tl - 1
		for i := 0; i < tl; i++ {
			if i == last {
				return m.keys[i].LessThan(t.keys[i])
			} else {
				if m.keys[i].LessThan(t.keys[i]) {
					return true
				}
			}
		}

		return false
	} else if tl < l {
		last := tl - 1
		for i := 0; i < tl; i++ {
			if i == last {
				return m.keys[i].LessThanItem(t.keys[i], m.item)
			} else {
				if m.keys[i].LessThan(t.keys[i]) {
					return true
				}
			}
		}

		return false
	} else {
		last := l - 1
		for i := 0; i < tl; i++ {
			if i == last {
				return m.keys[i].LessThan(t.keys[i])
			} else {
				if m.keys[i].LessThan(t.keys[i]) {
					return true
				}
			}
		}

		return true
	}
}

//type multi2IndexItem struct {
//	indexItem
//	key Key
//	key2 Key
//}
//func (m *multi2IndexItem) Less(than btree.Item, ctx interface{}) bool {
//	t := than.(*multi2IndexItem)
//	_ = t
//	return false
//}
//
//type multi3IndexItem struct {
//	indexItem
//	key Key
//	key2 Key
//	key3 Key
//}
