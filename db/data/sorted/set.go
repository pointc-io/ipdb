package sorted

import (
	"sync"
	"errors"
	"github.com/pointc-io/ipdb/db/data/btree"
	"github.com/pointc-io/ipdb/db/data/rtree"
	"github.com/pointc-io/ipdb/db/data/grect"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an item or idx is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an idx already exists in the database.
	ErrIndexExists = errors.New("idx exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is returned when post-loading data from an database
	// not opened with Open(":memory:").
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")
)

// Default number of btree degrees
//const btreeDegrees = 64
const btreeDegrees = 32

// exctx is a simple b-tree context for ordering by expiration.
type exctx struct {
	db *Set
}

var defaultFreeList = new(btree.FreeList)

//
type Set struct {
	commitIndex uint64

	items   *btree.BTree
	idxs    map[string]*index
	exps    *btree.BTree
	closed  bool
	mu      sync.RWMutex
}

func New() *Set {
	s := &Set{}
	s.items = btree.NewWithFreeList(btreeDegrees, defaultFreeList, nil)
	s.exps = btree.NewWithFreeList(btreeDegrees, defaultFreeList, nil)
	s.idxs = make(map[string]*index)
	return s
}

func (db *Set) insertIndex(idx *index) {
	db.idxs[idx.name] = idx
}

func (db *Set) removeIndex(idx *index) {
	// delete from the map.
	// this is all that is needed to delete an idx.
	delete(db.idxs, idx.name)
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *Set) View(fn func() error) error {
	db.mu.RLock()
	err := fn()
	db.mu.RUnlock()
	return err
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *Set) Update(fn func() error) error {
	db.mu.Lock()
	err := fn()
	db.mu.Unlock()
	return err
}

// get return an item or nil if not found.
func (db *Set) get(key string) *Item {
	item := db.items.Get(&Item{Key: key})
	if item != nil {
		return item.(*Item)
	}
	return nil
}

// DeleteAll deletes all items from the database.
func (db *Set) DeleteAll() error {
	// now reset the live database trees
	db.items = btree.NewWithFreeList(btreeDegrees, defaultFreeList, nil)
	db.exps = btree.NewWithFreeList(btreeDegrees, defaultFreeList, &exctx{db})
	db.idxs = make(map[string]*index)
	return nil
}

// insert performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *Set) insert(item *Item) *Item {
	var pdbi *Item
	prev := db.items.ReplaceOrInsert(item)
	//_i := -1
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*Item)
		item.indexes = pdbi.indexes

		if pdbi.Expires > 0 {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}

		for _, sec := range item.indexes {
			if sec != nil {
				sec.index().remove(sec)
			}
		}
	}
	if item.Expires > 0 {
		// The new item has eviction options. Add it to the
		// expires tree
		db.exps.ReplaceOrInsert(item)
	}
	for _, idx := range db.idxs {
		if !idx.match(item.Key) {
			continue
		}

		sk := idx.factory(idx, item)
		if sk == nil {
			continue
		}

		item.indexes = append(item.indexes, sk)

		if idx.btr != nil {
			if sk != nil {
				idx.btr.ReplaceOrInsert(sk)
			} else {
				// Ignored.
			}
		} else if idx.rtr != nil {
			if sk != nil {
				idx.rtr.Insert(sk)
			} else {
				// Ignored.
			}
		}
	}
	// we must return the previous item to the caller.
	return pdbi
}

// delete removes and item from the database and indexes. The input
// item must only have the key field specified thus "&dbItem{key: key}" is all
// that is needed to fully remove the item with the matching key. If an item
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *Set) delete(item *Item) *Item {
	var pdbi *Item
	prev := db.items.Delete(item)
	if prev != nil {
		pdbi = prev.(*Item)
		if pdbi.Expires > 0 {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, sec := range pdbi.indexes {
			if sec != nil {
				sec.index().remove(sec)
			}
		}
	}
	return pdbi
}

func (db *Set) Set(key, value string, expires int64) (previousValue string,
	replaced bool, err error) {

	item := &Item{Key: key, Value: value}
	if expires > 0 {
		// The caller is requesting that this item expires. Convert the
		// TTL to an absolute time and bind it to the item.
		item.Expires = expires
		//item.opts = &dbItemOpts{ex: true, exat: time.Now().Add(opts.TTL)}
	}
	// Insert the item into the keys tree.
	prev := db.insert(item)

	if prev == nil {
		return "", false, nil
	} else {
		return prev.Value, true, nil
	}
}

// Get returns a value for a key. If the item does not exist or if the item
// has expired then ErrNotFound is returned.
func (db *Set) Get(key string) (val string, err error) {
	if db == nil {
		return "", ErrTxClosed
	}
	item := db.get(key)
	if item == nil || item.expired() {
		// The item does not exists or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.Value, nil
}

// Delete removes an item from the database based on the item's key. If the item
// does not exist or if the item has expired then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (db *Set) Delete(key string) (val string, err error) {

	item := db.delete(&Item{Key: key})
	if item == nil {
		return "", ErrNotFound
	}

	// Even though the item has been deleted, we still want to check
	// if it has expired. An expired item should not be returned.
	if item.expired() {
		// The item exists in the tree, but has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.Value, nil
}

func (db *Set) scan(desc, gt, lt bool, index, start, stop string,
	iterator func(key string, value *Item) bool) error {
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		dbi := item.(*Item)
		return iterator(dbi.Key, dbi)
	}
	var tr *btree.BTree
	if index == "" {
		// empty idx means we will use the keys tree.
		tr = db.items
	} else {
		idx := db.idxs[index]
		if idx == nil {
			// idx was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *Item
	if gt || lt {
		if index == "" {
			itemA = &Item{Key: start}
			itemB = &Item{Key: stop}
		} else {
			itemA = &Item{Value: start}
			itemB = &Item{Value: stop}
			if desc {
				//itemA.keyless = true
				//itemB.keyless = true
			}
		}
	}

	if desc {
		if gt {
			if lt {
				tr.DescendRange(itemA, itemB, iter)
			} else {
				tr.DescendGreaterThan(itemA, iter)
			}
		} else if lt {
			tr.DescendLessOrEqual(itemA, iter)
		} else {
			tr.Descend(iter)
		}
	} else {
		if gt {
			if lt {
				tr.AscendRange(itemA, itemB, iter)
			} else {
				tr.AscendGreaterOrEqual(itemA, iter)
			}
		} else if lt {
			tr.AscendLessThan(itemA, iter)
		} else {
			tr.Ascend(iter)
		}
	}
	return nil
}

func (db *Set) scanSecondary(desc, gt, lt bool, index, start, stop string,
	iterator func(key string, value *Item) bool) error {
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		dbi := item.(*stringKey)
		return iterator(dbi.key, dbi.item)
	}
	var tr *btree.BTree
	if index == "" {
		// empty idx means we will use the keys tree.
		tr = db.items
	} else {
		idx := db.idxs[index]
		if idx == nil {
			// idx was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *stringKey
	if gt || lt {
		itemA = &stringKey{key: start}
		itemB = &stringKey{key: stop}
	}

	if desc {
		if gt {
			if lt {
				tr.DescendRange(itemA, itemB, iter)
			} else {
				tr.DescendGreaterThan(itemA, iter)
			}
		} else if lt {
			tr.DescendLessOrEqual(itemA, iter)
		} else {
			tr.Descend(iter)
		}
	} else {
		if gt {
			if lt {
				tr.AscendRange(itemA, itemB, iter)
			} else {
				tr.AscendGreaterOrEqual(itemA, iter)
			}
		} else if lt {
			tr.AscendLessThan(itemA, iter)
		} else {
			tr.Ascend(iter)
		}
	}
	return nil
}

// Nearby searches for rectangle items that are nearby a target rect.
// All items belonging to the specified idx will be returned in order of
// nearest to farthest.
// The specified idx must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid idx will return an error.
func (db *Set) Nearby(index, bounds string,
	iterator func(key *rectKey, value *Item, dist float64) bool) error {
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// // wrap a rtree specific iterator around the user-defined iterator.
	iter := func(item rtree.Item, dist float64) bool {
		dbi := item.(*rectKey)
		return iterator(dbi, dbi.item, dist)
	}
	idx := db.idxs[index]
	if idx == nil {
		// idx was not found. return error
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree idx. just return nil
		return nil
	}
	// execute the nearby search

	// set the center param to false, which uses the box dist calc.
	idx.rtr.KNN(grect.Get(bounds), false, iter)
	return nil
}

// Intersects searches for rectangle items that intersect a target rect.
// The specified idx must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid idx will return an error.
func (db *Set) Intersects(index, bounds string,
	iterator func(key *rectKey, value *Item) bool) error {
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// wrap a rtree specific iterator around the user-defined iterator.
	iter := func(item rtree.Item) bool {
		dbi := item.(*rectKey)
		return iterator(dbi, dbi.item)
	}
	idx := db.idxs[index]
	if idx == nil {
		// idx was not found. return error
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree idx. just return nil
		return nil
	}

	idx.rtr.Search(grect.Get(bounds), iter)
	return nil
}

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// When an idx is provided, the results will be ordered by the item values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the item key.
// An invalid idx will return an error.
func (db *Set) Ascend(index string,
	iterator func(key string, value *Item) bool) error {
	if index == "" {
		return db.scan(false, false, false, index, "", "", iterator)
	} else {
		return db.scanSecondary(false, false, false, index, "", "", iterator)
	}
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// When an idx is provided, the results will be ordered by the item values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the item key.
// An invalid idx will return an error.
func (db *Set) AscendGreaterOrEqual(index, pivot string,
	iterator func(key string, value *Item) bool) error {
	if index == "" {
		return db.scan(false, true, false, index, pivot, "", iterator)
	} else {
		return db.scanSecondary(false, true, false, index, pivot, "", iterator)
	}
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// When an idx is provided, the results will be ordered by the item values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the item key.
// An invalid idx will return an error.
func (db *Set) AscendLessThan(index, pivot string,
	iterator func(key string, value *Item) bool) error {
	if index == "" {
		return db.scan(false, false, true, index, pivot, "", iterator)
	} else {
		return db.scanSecondary(false, false, true, index, pivot, "", iterator)
	}
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an idx is provided, the results will be ordered by the item values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the item key.
// An invalid idx will return an error.
func (db *Set) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key string, value *Item) bool) error {
	if index == "" {
		return db.scan(
			false, true, true, index, greaterOrEqual, lessThan, iterator)
	} else {
		return db.scanSecondary(
			false, true, true, index, greaterOrEqual, lessThan, iterator)
	}
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// When an idx is provided, the results will be ordered by the item values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the item key.
// An invalid idx will return an error.
func (db *Set) Descend(index string,
	iterator func(key string, value *Item) bool) error {
	if index == "" {
		return db.scan(true, false, false, index, "", "", iterator)
	} else {
		return db.scanSecondary(true, false, false, index, "", "", iterator)
	}
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// When an idx is provided, the results will be ordered by the item values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the item key.
// An invalid idx will return an error.
func (db *Set) DescendGreaterThan(index, pivot string,
	iterator func(key string, value *Item) bool) error {
	if index == "" {
		return db.scan(true, true, false, index, pivot, "", iterator)
	} else {
		return db.scanSecondary(true, true, false, index, pivot, "", iterator)
	}
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// When an idx is provided, the results will be ordered by the item values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the item key.
// An invalid idx will return an error.
func (db *Set) DescendLessOrEqual(index, pivot string,
	iterator func(key string, value *Item) bool) error {
	if index == "" {
		return db.scan(true, false, true, index, pivot, "", iterator)
	} else {
		return db.scanSecondary(true, false, true, index, pivot, "", iterator)
	}
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an idx is provided, the results will be ordered by the item values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the item key.
// An invalid idx will return an error.
func (db *Set) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key string, value *Item) bool) error {
	if index == "" {
		return db.scan(
			true, true, true, index, lessOrEqual, greaterThan, iterator,
		)
	} else {
		return db.scanSecondary(
			true, true, true, index, lessOrEqual, greaterThan, iterator,
		)
	}
}

// Rect is helper function that returns a string representation
// of a rect. IndexRect() is the reverse function and can be used
// to generate a rect from a string.
func Rect(min, max []float64) string {
	r := grect.Rect{Min: min, Max: max}
	return r.String()
}

// Point is a helper function that converts a series of float64s
// to a rectangle for a spatial idx.
func Point(coords ...float64) string {
	return Rect(coords, coords)
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
func CaseInsensitiveCompare(a, b string) bool {
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
