package item

import (
	"sync"
	"errors"
	"github.com/pointc-io/ipdb/index/btree"
	"github.com/pointc-io/ipdb/index/rtree"
	//"github.com/pointc-io/ipdb/data/sorted"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an value or idx is not in the database.
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


// exctx is a simple b-tree context for ordering by expiration.
type exctx struct {
	db *Set
}

var defaultFreeList = new(btree.FreeList)

//
type Set struct {
	commitIndex uint64

	items  *btree.BTree
	idxs   map[string]*Index
	exps   *btree.BTree
	closed bool
	mu     sync.RWMutex
}

func New() *Set {
	s := &Set{}
	s.items = btree.NewWithFreeList(btreeDegrees, defaultFreeList, nil)
	s.exps = btree.NewWithFreeList(btreeDegrees, defaultFreeList, nil)
	s.idxs = make(map[string]*Index)
	return s
}

func (db *Set) insertIndex(idx *Index) {
	db.idxs[idx.name] = idx
}

func (db *Set) removeIndex(idx *Index) {
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

// get return an value or nil if not found.
func (db *Set) get(key Key) *ValueItem {
	item := db.items.Get(key)
	if item != nil {
		return item.(*ValueItem)
	}
	return nil
}

// DeleteAll deletes all items from the database.
func (db *Set) DeleteAll() error {
	// now reset the live database trees
	db.items = btree.NewWithFreeList(btreeDegrees, defaultFreeList, nil)
	db.exps = btree.NewWithFreeList(btreeDegrees, defaultFreeList, &exctx{db})
	db.idxs = make(map[string]*Index)
	return nil
}

// insert performs inserts an value in to the database and updates
// all indexes. If a previous value with the same key already exists, that value
// will be replaced with the new one, and return the previous value.
func (db *Set) insert(item *ValueItem) *ValueItem {
	var pdbi *ValueItem
	prev := db.items.ReplaceOrInsert(item)
	//_i := -1
	if prev != nil {
		// A previous value was removed from the keys tree. Let's
		// fully delete this value from all indexes.
		pdbi = prev.(*ValueItem)
		item.Indexes = pdbi.Indexes

		if pdbi.Expires > 0 {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}

		for _, sec := range item.Indexes {
			if sec != nil {
				sec.index().remove(sec)
			}
		}
	}
	if item.Expires > 0 {
		// The new value has eviction options. Add it to the
		// expires tree
		db.exps.ReplaceOrInsert(item)
	}
	for _, idx := range db.idxs {
		if !idx.match(item.Key) {
			continue
		}

		sk := idx.indexer.Index(idx, item)
		if sk == nil {
			continue
		}

		item.Indexes = append(item.Indexes, sk)

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
	// we must return the previous value to the caller.
	return pdbi
}

// delete removes and value from the database and indexes. The input
// value must only have the key field specified thus "&dbItem{key: key}" is all
// that is needed to fully remove the value with the matching key. If an value
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the value was not
// found in the database
func (db *Set) delete(item *ValueItem) *ValueItem {
	var pdbi *ValueItem
	prev := db.items.Delete(item)
	if prev != nil {
		pdbi = prev.(*ValueItem)
		if pdbi.Expires > 0 {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, sec := range pdbi.Indexes {
			if sec != nil {
				sec.index().remove(sec)
			}
		}
	}
	return pdbi
}

//
//
//
func (db *Set) Set(key Key, value string, expires int64) (previousValue string,
	replaced bool, err error) {

	item := &ValueItem{Key: key, Value: value}
	if expires > 0 {
		// The caller is requesting that this value expires. Convert the
		// TTL to an absolute time and bind it to the value.
		item.Expires = expires
		//value.opts = &dbItemOpts{ex: true, exat: time.Now().Add(opts.TTL)}
	}
	// Insert the value into the keys tree.
	prev := db.insert(item)

	if prev == nil {
		return "", false, nil
	} else {
		return prev.Value, true, nil
	}
}

// Get returns a value for a key. If the value does not exist or if the value
// has expired then ErrNotFound is returned.
func (db *Set) Get(key Key) (val string, err error) {
	if db == nil {
		return "", ErrTxClosed
	}
	item := db.get(key)
	if item == nil || item.expired() {
		// The value does not exists or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.Value, nil
}

// Delete removes an value from the database based on the value's key. If the value
// does not exist or if the value has expired then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (db *Set) Delete(key Key) (val string, err error) {

	item := db.delete(&ValueItem{Key: key})
	if item == nil {
		return "", ErrNotFound
	}

	// Even though the value has been deleted, we still want to check
	// if it has expired. An expired value should not be returned.
	if item.expired() {
		// The value exists in the tree, but has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.Value, nil
}

//
//
//
func (db *Set) scanPrimary(desc, gt, lt bool, start, stop Key,
	iterator func(value *ValueItem) bool) error {
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		switch dbi := item.(type) {
		case *ValueItem:
			return iterator(dbi)
		}
		return false
	}

	// create some limit items
	var itemA, itemB Key
	if gt || lt {
		itemA = start
		itemB = stop
	}

	if desc {
		if gt {
			if lt {
				db.items.DescendRange(itemA, itemB, iter)
			} else {
				db.items.DescendGreaterThan(itemA, iter)
			}
		} else if lt {
			db.items.DescendLessOrEqual(itemA, iter)
		} else {
			db.items.Descend(iter)
		}
	} else {
		if gt {
			if lt {
				db.items.AscendRange(itemA, itemB, iter)
			} else {
				db.items.AscendGreaterOrEqual(itemA, iter)
			}
		} else if lt {
			db.items.AscendLessThan(itemA, iter)
		} else {
			db.items.Ascend(iter)
		}
	}

	return nil
}

//
//
//
func (db *Set) scanSecondary(desc, gt, lt bool, index string, start, stop Key,
	iterator func(key IndexItem) bool) error {
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		dbi, ok := item.(IndexItem)
		if !ok {
			return false
		} else {
			return iterator(dbi)
		}
	}

	idx := db.idxs[index]
	if idx == nil {
		// idx was not found. return error
		return ErrNotFound
	}
	tr := idx.btr
	if tr == nil {
		return nil
	}

	// create some limit items
	var itemA, itemB Key
	if gt || lt {
		itemA = start
		itemB = stop
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
	iterator func(key *RectItem, value *ValueItem, dist float64) bool) error {
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// // wrap a rtree specific iterator around the user-defined iterator.
	iter := func(item rtree.Item, dist float64) bool {
		dbi, ok := item.(*RectItem)
		if !ok {
			return true
		}
		return iterator(dbi, dbi.value, dist)
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
	idx.rtr.KNN(ParseRect(bounds), false, iter)
	return nil
}

// Intersects searches for rectangle items that intersect a target rect.
// The specified idx must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid idx will return an error.
func (db *Set) Intersects(index, bounds string,
	iterator func(key *RectItem, value *ValueItem) bool) error {
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// wrap a rtree specific iterator around the user-defined iterator.
	iter := func(item rtree.Item) bool {
		dbi := item.(*RectItem)
		return iterator(dbi, dbi.value)
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

	idx.rtr.Search(ParseRect(bounds), iter)
	return nil
}

// Ascend calls the iterator for every value in the database within the range
// [first, last], until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) AscendPrimary(iterator func(value *ValueItem) bool) error {
	return db.scanPrimary(false, false, false, MinKey, MaxKey, iterator)
}

// AscendGreaterOrEqual calls the iterator for every value in the database within
// the range [pivot, last], until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) AscendGreaterOrEqualPrimary(pivot Key,
	iterator func(value *ValueItem) bool) error {
	return db.scanPrimary(false, true, false, pivot, MaxKey, iterator)
}

// AscendLessThan calls the iterator for every value in the database within the
// range [first, pivot), until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) AscendLessThanPrimary(pivot Key,
	iterator func(value *ValueItem) bool) error {
	return db.scanPrimary(false, false, true, pivot, MaxKey, iterator)
}

// AscendRange calls the iterator for every value in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) AscendRangePrimary(greaterOrEqual, lessThan Key,
	iterator func(value *ValueItem) bool) error {
	return db.scanPrimary(
		false, true, true, greaterOrEqual, lessThan, iterator)
}

// Descend calls the iterator for every value in the database within the range
// [last, first], until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) DescendPrimary(iterator func(value *ValueItem) bool) error {
	return db.scanPrimary(true, false, false, MinKey, MinKey, iterator)
}

// DescendGreaterThan calls the iterator for every value in the database within
// the range [last, pivot), until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) DescendGreaterThanPrimary(pivot Key,
	iterator func(value *ValueItem) bool) error {
	return db.scanPrimary(true, true, false, pivot, MinKey, iterator)
}

// DescendLessOrEqual calls the iterator for every value in the database within
// the range [pivot, first], until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) DescendLessOrEqualPrimary(pivot Key,
	iterator func(value *ValueItem) bool) error {
	return db.scanPrimary(true, false, true, pivot, MinKey, iterator)
}

// DescendRange calls the iterator for every value in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) DescendRangePrimary(lessOrEqual, greaterThan Key,
	iterator func(value *ValueItem) bool) error {
	return db.scanPrimary(
		true, true, true, lessOrEqual, greaterThan, iterator,
	)
}

// Ascend calls the iterator for every value in the database within the range
// [first, last], until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) Ascend(index string, iterator IndexIterator) error {
	return db.scanSecondary(false, false, false, index, MinKey, MaxKey, iterator)
}

// AscendGreaterOrEqual calls the iterator for every value in the database within
// the range [pivot, last], until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) AscendGreaterOrEqual(index string, pivot Key, iterator IndexIterator) error {
	return db.scanSecondary(false, true, false, index, pivot, MinKey, iterator)
}

// AscendLessThan calls the iterator for every value in the database within the
// range [first, pivot), until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) AscendLessThan(index string, pivot Key, iterator IndexIterator) error {
	return db.scanSecondary(false, false, true, index, pivot, MinKey, iterator)
}

// AscendRange calls the iterator for every value in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) AscendRange(index string, greaterOrEqual, lessThan Key, iterator IndexIterator) error {
	return db.scanSecondary(
		false, true, true, index, greaterOrEqual, lessThan, iterator)
}

// Descend calls the iterator for every value in the database within the range
// [last, first], until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) Descend(index string, iterator IndexIterator) error {
	return db.scanSecondary(true, false, false, index, MaxKey, MinKey, iterator)
}

// DescendGreaterThan calls the iterator for every value in the database within
// the range [last, pivot), until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) DescendGreaterThan(index string, pivot Key, iterator IndexIterator) error {
	return db.scanSecondary(true, true, false, index, pivot, MinKey, iterator)
}

// DescendLessOrEqual calls the iterator for every value in the database within
// the range [pivot, first], until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) DescendLessOrEqual(index string, pivot Key, iterator IndexIterator) error {
	return db.scanSecondary(true, false, true, index, pivot, MinKey, iterator)
}

// DescendRange calls the iterator for every value in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an idx is provided, the results will be ordered by the value values
// as specified by the less() function of the defined idx.
// When an idx is not provided, the results will be ordered by the value key.
// An invalid idx will return an error.
func (db *Set) DescendRange(index string, lessOrEqual, greaterThan Key, iterator IndexIterator) error {
	return db.scanSecondary(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

//// Point is a helper function that converts a series of float64s
//// to a rectangle for a spatial idx.
//func Point(coords ...float64) string {
//	return Rect(coords, coords)
//}

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
