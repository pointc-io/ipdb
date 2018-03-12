package sorted

import (
	"strconv"

	"io"
	"sync"
	"time"
	"errors"
	"strings"
	"unsafe"
	"encoding/binary"

	"github.com/tidwall/match"
	"github.com/pointc-io/ipdb/codec/gjson"
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

	// ErrNotFound is returned when an item or index is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an index already exists in the database.
	ErrIndexExists = errors.New("index exists")

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
const btreeDegrees = 64

// exctx is a simple b-tree context for ordering by expiration.
type exctx struct {
	db *Set
}

var defaultFreeList = new(btree.FreeList)

//
type Set struct {
	commitIndex uint64

	items  *btree.BTree
	idxs   map[string]*index
	exps   *btree.BTree
	closed bool
	mu     sync.RWMutex
}

func New() *Set {
	s := &Set{}
	s.items = btree.NewWithFreeList(btreeDegrees, defaultFreeList, nil)
	s.exps = btree.NewWithFreeList(btreeDegrees, defaultFreeList, nil)
	s.idxs = make(map[string]*index)
	return s
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

// IndexOptions provides an index with additional features or
// alternate functionality.
type IndexOptions struct {
	// CaseInsensitiveKeyMatching allow for case-insensitive
	// matching on keys when setting key/values.
	CaseInsensitiveKeyMatching bool
}

type IndexType uint8

const (
	BTree IndexType = 0
	RTree IndexType = 1
)

// index represents a b-tree or r-tree index and also acts as the
// b-tree/r-tree context for itself.
type index struct {
	id      uint8
	t       IndexType
	btr     *btree.BTree                           // contains the items
	rtr     *rtree.RTree                           // contains the items
	name    string                                 // name of the index
	pattern string                                 // a required key pattern
	less    func(a, b string) bool                 // less comparison function
	rect    func(item string) (min, max []float64) // rect from string function
	db      *Set                                   // the origin database
	opts    IndexOptions                           // index options

	factory *jsonKeyFactory
}

// Snapshot of an index only includes meta-data to re-create the index since
// it is built from the items in the tree.
func (i *index) Snapshot(writer io.Writer) error {
	return nil
}

func (i *index) Restore(writer io.Writer) error {
	return nil
}

// match matches the pattern to the key
func (idx *index) match(key string) bool {
	if idx.pattern == "*" {
		return true
	}
	if idx.opts.CaseInsensitiveKeyMatching {
		for i := 0; i < len(key); i++ {
			if key[i] >= 'A' && key[i] <= 'Z' {
				key = strings.ToLower(key)
				break
			}
		}
	}
	return match.Match(key, idx.pattern)
}

// clearCopy creates a copy of the index, but with an empty dataset.
func (idx *index) clearCopy() *index {
	// copy the index meta information
	nidx := &index{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		rect:    idx.rect,
		opts:    idx.opts,
	}
	// initialize with empty trees
	if nidx.less != nil {
		nidx.btr = btree.New(btreeDegrees, nidx)
	}
	if nidx.rect != nil {
		nidx.rtr = rtree.New(nidx)
	}
	return nidx
}

// rebuild rebuilds the index
// needs to be invoked from a worker
func (idx *index) rebuild() {
	// initialize trees
	if idx.less != nil || idx.factory != nil {
		idx.btr = btree.New(btreeDegrees, idx)
	}
	if idx.rect != nil {
		idx.rtr = rtree.New(idx)
	}
	// iterate through all keys and fill the index
	idx.db.items.Ascend(func(item btree.Item) bool {
		dbi := item.(*Item)
		if !idx.match(dbi.Key) {
			// does not match the pattern, continue
			return true
		}

		if idx.factory != nil {
			var sk keyFactory
			if len(dbi.indexes) > 0 {
				for _, sec := range dbi.indexes {
					if sec.idx() == idx.id {
						sk = sec
						break
					}
				}
			}

			if sk == nil {
				sk = idx.factory.create(dbi)
				if sk != nil {
					dbi.indexes = append(dbi.indexes, sk)
				}
			}

			if sk != nil {
				idx.btr.ReplaceOrInsert(sk)
			} else {
				panic("Can't build index")
			}
		} else if idx.less != nil {
			idx.btr.ReplaceOrInsert(dbi)
		}
		if idx.rect != nil {
			//min, max := idx.rect("")
			//item := &rectItem{
			//	item: dbi,
			//	min:  min,
			//	max:  max,
			//}
			idx.rtr.Insert(dbi)
		}
		return true
	})
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

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *Set) insertIntoDatabase(item *Item) *Item {
	var pdbi *Item
	prev := db.items.ReplaceOrInsert(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*Item)
		if pdbi.Expires > 0 {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				if idx.factory != nil {
					var sk keyFactory
					if len(item.indexes) > 0 {
						for _, sec := range item.indexes {
							if sec.idx() == idx.id {
								sk = sec
								break
							}
						}
					}

					if sk != nil {
						idx.btr.Delete(sk)
					}
				} else if idx.less != nil {
					// Remove it from the btree index.
					idx.btr.Delete(pdbi)
				}
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
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
		if idx.btr != nil {
			if idx.factory != nil {
				var sk keyFactory
				if len(item.indexes) > 0 {
					for _, sec := range item.indexes {
						if sec.idx() == idx.id {
							sk = sec
							break
						}
					}
				}

				if sk == nil {
					sk = idx.factory.create(item)
					if sk != nil {
						item.indexes = append(item.indexes, sk)
					}
				}

				if sk != nil {
					idx.btr.ReplaceOrInsert(sk)
				} else {
					panic("Can't build index")
				}
			} else if idx.less != nil {
				// Add new item to btree index.
				idx.btr.ReplaceOrInsert(item)
			}
		}
		if idx.rtr != nil {
			// Add new item to rtree index.
			idx.rtr.Insert(item)
		}
	}
	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes and item from the database and indexes. The input
// item must only have the key field specified thus "&dbItem{key: key}" is all
// that is needed to fully remove the item with the matching key. If an item
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *Set) deleteFromDatabase(item *Item) *Item {
	var pdbi *Item
	prev := db.items.Delete(item)
	if prev != nil {
		pdbi = prev.(*Item)
		if pdbi.Expires > 0 {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				if idx.factory != nil {
					var sk keyFactory
					if len(item.indexes) > 0 {
						for _, sec := range item.indexes {
							if sec.idx() == idx.id {
								sk = sec
								break
							}
						}
					}

					if sk != nil {
						idx.btr.Delete(sk)
					}
				} else if idx.less != nil {
					// Remove it from the btree index.
					idx.btr.Delete(pdbi)
				}
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
			}
		}
	}
	return pdbi
}

// GetLess returns the less function for an index. This is handy for
// doing ad-hoc compares inside a transaction.
// Returns ErrNotFound if the index is not found or there is no less
// function bound to the index
func (db *Set) GetLess(index string) (func(a, b string) bool, error) {
	if db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := db.idxs[index]
	if !ok || idx.less == nil {
		return nil, ErrNotFound
	}
	return idx.less, nil
}

// GetRect returns the rect function for an index. This is handy for
// doing ad-hoc searches inside a transaction.
// Returns ErrNotFound if the index is not found or there is no rect
// function bound to the index
func (db *Set) GetRect(index string) (func(s string) (min, max []float64),
	error) {
	if db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := db.idxs[index]
	if !ok || idx.rect == nil {
		return nil, ErrNotFound
	}
	return idx.rect, nil
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
	prev := db.insertIntoDatabase(item)

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

	item := db.deleteFromDatabase(&Item{Key: key})
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
	iterator func(key, value string) bool) error {
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		switch dbi := item.(type) {
		case *Item:
			return iterator(dbi.Key, dbi.Value)
		case *jsonKeyItem:
			return iterator(dbi.key.Str, dbi.item.Value)
		}
		dbi := item.(*Item)
		return iterator(dbi.Key, dbi.Value)
	}
	var tr *btree.BTree
	if index == "" {
		// empty index means we will use the keys tree.
		tr = db.items
	} else {
		idx := db.idxs[index]
		if idx == nil {
			// index was not found. return error
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
	iterator func(key, value string) bool) error {
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		switch dbi := item.(type) {
		case *Item:
			return iterator(dbi.Key, dbi.Value)
		case *jsonKeyItem:
			return iterator(dbi.key.Raw, dbi.item.Value)
		}
		dbi := item.(*Item)
		return iterator(dbi.Key, dbi.Value)
	}
	var tr *btree.BTree
	if index == "" {
		// empty index means we will use the keys tree.
		tr = db.items
	} else {
		idx := db.idxs[index]
		if idx == nil {
			// index was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *jsonKeyItem
	if gt || lt {
		if index == "" {
			itemA = &jsonKeyItem{key: gjson.Result{Str: start, Type: gjson.String}}
			itemB = &jsonKeyItem{key: gjson.Result{Str: stop, Type: gjson.String}}
		} else {
			//itemA = &Item{Value: start}
			//itemB = &Item{Value: stop}
			if desc {
				//itemA.keyless = true
				//itemB.keyless = true
			}
		}
	}

	//itemA = &jsonKeyItem{key: gjson.Result{Str: start, Type: gjson.String}}
	//itemB = &jsonKeyItem{key: gjson.Result{Str: stop, Type: gjson.String}}
	// execute the scan on the underlying tree.
	//if tx.wc != nil {
	//	tx.wc.itercount++
	//	defer func() {
	//		tx.wc.itercount--
	//	}()
	//}
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

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (db *Set) Ascend(index string,
	iterator func(key, value string) bool) error {
	return db.scan(false, false, false, index, "", "", iterator)
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (db *Set) AscendGreaterOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return db.scan(false, true, false, index, pivot, "", iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (db *Set) AscendLessThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return db.scan(false, false, true, index, pivot, "", iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (db *Set) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key, value string) bool) error {
	return db.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (db *Set) Descend(index string,
	iterator func(key, value string) bool) error {
	return db.scan(true, false, false, index, "", "", iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (db *Set) DescendGreaterThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return db.scan(true, true, false, index, pivot, "", iterator)
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (db *Set) DescendLessOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return db.scan(true, false, true, index, pivot, "", iterator)
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (db *Set) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key, value string) bool) error {
	return db.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (db *Set) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.createIndex(name, pattern, less, nil, nil)
}

// CreateIndexOptions is the same as CreateIndex except that it allows
// for additional options.
func (db *Set) CreateIndexOptions(name, pattern string,
	opts *IndexOptions,
	less ...func(a, b string) bool) error {
	return db.createIndex(name, pattern, less, nil, opts)
}

// CreateSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// An error will occur if an index with the same name already exists.
//
// The rect function converts a string to a rectangle. The rectangle is
// represented by two arrays, min and max. Both arrays may have a length
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// There is support for up to 20 dimensions.
// The values of min must be less than the values of max at the same dimension.
// Thus min[0] must be less-than-or-equal-to max[0].
// The IndexRect is a default function that can be used for the rect
// parameter.
func (db *Set) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.createIndex(name, pattern, nil, rect, nil)
}

// CreateSpatialIndexOptions is the same as CreateSpatialIndex except that
// it allows for additional options.
func (db *Set) CreateSpatialIndexOptions(name, pattern string,
	opts *IndexOptions,
	rect func(item string) (min, max []float64)) error {
	return db.createIndex(name, pattern, nil, rect, nil)
}

// createIndex is called by CreateIndex() and CreateSpatialIndex()
func (db *Set) createIndex(name string, pattern string,
	lessers []func(a, b string) bool,
	rect func(item string) (min, max []float64),
	opts *IndexOptions,
) error {
	if name == "" {
		// cannot create an index without a name.
		// an empty name index is designated for the main "keys" tree.
		return ErrIndexExists
	}
	// check if an index with that name already exists.
	if _, ok := db.idxs[name]; ok {
		// index with name already exists. error.
		return ErrIndexExists
	}
	// genreate a less function
	var less func(a, b string) bool
	switch len(lessers) {
	default:
		// multiple less functions specified.
		// create a compound less function.
		less = func(a, b string) bool {
			for i := 0; i < len(lessers)-1; i++ {
				if lessers[i](a, b) {
					return true
				}
				if lessers[i](b, a) {
					return false
				}
			}
			return lessers[len(lessers)-1](a, b)
		}
	case 0:
		// no less function
	case 1:
		less = lessers[0]
	}
	var sopts IndexOptions
	if opts != nil {
		sopts = *opts
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	// intialize new index
	idx := &index{
		name:    name,
		pattern: pattern,
		less:    less,
		rect:    rect,
		db:      db,
		opts:    sopts,
	}
	idx.rebuild()
	// save the index
	db.idxs[name] = idx

	return nil
}

func (db *Set) CreateJsonIndex(name, pattern string, path string) error {
	return db.createSecondaryIndex(name, pattern, &jsonKeyFactory{path: path}, nil)
}

// createIndex is called by CreateIndex() and CreateSpatialIndex()
func (db *Set) createSecondaryIndex(name string, pattern string,
	factory *jsonKeyFactory,
	opts *IndexOptions,
) error {
	if name == "" {
		// cannot create an index without a name.
		// an empty name index is designated for the main "keys" tree.
		return ErrIndexExists
	}
	// check if an index with that name already exists.
	if _, ok := db.idxs[name]; ok {
		// index with name already exists. error.
		return ErrIndexExists
	}

	var sopts IndexOptions
	if opts != nil {
		sopts = *opts
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	// intialize new index
	idx := &index{
		name:    name,
		pattern: pattern,
		//less:    less,
		//rect:    rect,
		db:      db,
		opts:    sopts,
		factory: factory,
	}
	factory.index = idx
	idx.rebuild()
	// save the index
	db.idxs[name] = idx
	//if tx.wc.rbkeys == nil {
	//	// store the index in the rollback map.
	//	if _, ok := tx.wc.rollbackIndexes[name]; !ok {
	//		// we use nil to indicate that the index should be removed upon rollback.
	//		tx.wc.rollbackIndexes[name] = nil
	//	}
	//}
	return nil
}

// DropIndex removes an index.
func (db *Set) DropIndex(name string) error {
	if name == "" {
		// cannot drop the default "keys" index
		return ErrInvalidOperation
	}
	_, ok := db.idxs[name]
	if !ok {
		return ErrNotFound
	}
	// delete from the map.
	// this is all that is needed to delete an index.
	delete(db.idxs, name)
	return nil
}

// Rect is helper function that returns a string representation
// of a rect. IndexRect() is the reverse function and can be used
// to generate a rect from a string.
func Rect(min, max []float64) string {
	r := grect.Rect{Min: min, Max: max}
	return r.String()
}

// Point is a helper function that converts a series of float64s
// to a rectangle for a spatial index.
func Point(coords ...float64) string {
	return Rect(coords, coords)
}

// IndexRect is a helper function that converts string to a rect.
// Rect() is the reverse function and can be used to generate a string
// from a rect.
func IndexRect(a string) (min, max []float64) {
	r := grect.Get(a)
	return r.Min, r.Max
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
func IndexString(a, b string) bool {
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

// IndexBinary is a helper function that returns true if 'a' is less than 'b'.
// This compares the raw binary of the string.
func IndexBinary(a, b string) bool {
	return a < b
}

// IndexInt is a helper function that returns true if 'a' is less than 'b'.
func IndexInt(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia < ib
}

// IndexInt is a helper function that returns true if 'a' is less than 'b'.
func IndexIntDup(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia <= ib
}

// IndexUint is a helper function that returns true if 'a' is less than 'b'.
// This compares uint64s that are added to the database using the
// Uint() conversion function.
func IndexUint(a, b string) bool {
	ia, _ := strconv.ParseUint(a, 10, 64)
	ib, _ := strconv.ParseUint(b, 10, 64)
	return ia < ib
}

// IndexFloat is a helper function that returns true if 'a' is less than 'b'.
// This compares float64s that are added to the database using the
// Float() conversion function.
func IndexFloat(a, b string) bool {
	ia, _ := strconv.ParseFloat(a, 64)
	ib, _ := strconv.ParseFloat(b, 64)
	return ia < ib
}

// IndexJSON provides for the ability to create an index on any JSON field.
// When the field is a string, the comparison will be case-insensitive.
// It returns a helper function used by CreateIndex.
func IndexJSONDup(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).LessDup(gjson.Get(b, path), false)
	}
}

// IndexJSON provides for the ability to create an index on any JSON field.
// When the field is a string, the comparison will be case-insensitive.
// It returns a helper function used by CreateIndex.
func IndexJSON(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), false)
	}
}

// IndexJSONCaseSensitive provides for the ability to create an index on
// any JSON field.
// When the field is a string, the comparison will be case-sensitive.
// It returns a helper function used by CreateIndex.
func IndexJSONCaseSensitive(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), true)
	}
}

// Desc is a helper function that changes the order of an index.
func Desc(less func(a, b string) bool) func(a, b string) bool {
	return func(a, b string) bool { return less(b, a) }
}

//type dbItemOpts struct {
//	ex   bool      // does this item expire?
//	exat time.Time // when does this item expire?
//}

type Indexer interface {
	btree.Item
	rtree.Item

	Remove()
}

type rectItem struct {
	item *Item
	min  []float64
	max  []float64
}

func (dbi *rectItem) Less(than btree.Item, ctx interface{}) bool {
	t := than.(*rectItem)
	minl := len(dbi.min)
	maxl := len(dbi.max)
	tminl := len(t.min)
	tmaxl := len(t.max)

	_ = maxl
	_ = tmaxl

	if minl == 0 {
		if tminl == 0 {
			return false
		} else {
			return true
		}
	} else {
		if tminl == 0 {
			return false
		} else {
			return dbi.min[0] < t.min[0]
		}
	}
}

func (dbi *rectItem) Rect(ctx interface{}) (min []float64, max []float64) {
	return dbi.min, dbi.max
}

type Item struct {
	Type    uint8
	Key     string
	Value   string
	Expires int64

	indexes []keyFactory
}

//func (dbi *Item) GetRect() (min []float64, max []float64) {
//	return []float64{0.0}, []float64{0.0}
//}

func (dbi *Item) Rect(ctx interface{}) (min []float64, max []float64) {
	switch ctx := ctx.(type) {
	case *index:
		return ctx.rect(*(*string)(unsafe.Pointer(&dbi.Value)))
	}

	return nil, nil
}

func (dbi *Item) Less(than btree.Item, ctx interface{}) bool {
	switch ctx := ctx.(type) {
	case *index:
		return ctx.less(dbi.Value, than.(*Item).Value)
	}
	return dbi.Value < than.(*Item).Value
}

func (dbi *Item) AppendValue(buf []byte) []byte {
	return buf
}

func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(count), 10)...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = append(buf, strconv.FormatInt(int64(len(s)), 10)...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// writeSetTo writes an item as a single SET record to the a bufio Writer.
func (dbi *Item) writeSetTo(buf []byte) []byte {
	if dbi.Expires > 0 {
		ex := dbi.Expires
		buf = appendArray(buf, 5)
		buf = appendBulkString(buf, "SET")
		buf = appendBulkString(buf, dbi.Key)
		buf = appendBulkString(buf, string(dbi.AppendValue(nil)))
		buf = appendBulkString(buf, "EX")
		buf = appendBulkString(buf, strconv.FormatUint(uint64(ex), 10))
	} else {
		buf = appendArray(buf, 3)
		buf = appendBulkString(buf, "SET")
		buf = appendBulkString(buf, dbi.Key)
		buf = appendBulkString(buf, string(dbi.AppendValue(nil)))
	}
	return buf
}

// writeSetTo writes an item as a single DEL record to the a bufio Writer.
func (dbi *Item) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkString(buf, dbi.Key)
	return buf
}

// expired evaluates id the item has expired. This will always return false when
// the item does not have `opts.ex` set to true.
func (dbi *Item) expired() bool {
	return dbi.Expires > 0 && time.Now().Unix() > dbi.Expires
	//return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// expiresAt will return the time when the item will expire. When an item does
// not expire `maxTime` is used.
func (dbi *Item) expiresAt() int64 {
	return dbi.Expires
}

//
//
//
type keyFactory interface {
	btree.Item

	idx() uint8
}

type jsonKeyFactory struct {
	index *index
	path  string
}

func (s *jsonKeyFactory) create(item *Item) keyFactory {
	result := gjson.Get(item.Value, s.path)

	return &jsonKeyItem{
		key:     result,
		item:    item,
		factory: s,
	}
}

//
//
//
type jsonKeyItem struct {
	key     gjson.Result
	item    *Item
	factory *jsonKeyFactory
}

func (s *jsonKeyItem) update() gjson.Result {
	result := gjson.Get(s.item.Value, s.factory.path)
	return result
}

func (s *jsonKeyItem) idx() uint8 {
	return s.factory.index.id
}

func (k *jsonKeyItem) Less(than btree.Item, ctx interface{}) bool {
	t := than.(*jsonKeyItem)
	return k.key.LessTieBreaker(t.key, true, k.item.Key, t.item.Key)
	//return k.key.Less(t.key, true)
}

type jsonSpatialItem struct {
}

////
//type SecondaryFloat64 struct {
//	pk   string
//	key  float64
//	item *Item
//}
//
////
//func (k *SecondaryFloat64) Less(than btree.Item, ctx interface{}) bool {
//	t := than.(*SecondaryFloat64)
//	if k.key < t.key {
//		return true
//	} else if k.key == t.key {
//		return k.pk < t.pk
//	} else {
//		return false
//	}
//}
//
//type SecondaryInt64 struct {
//	pk   string
//	key  int64
//	item *Item
//}
//
//func (k *SecondaryInt64) Less(than btree.Item, ctx interface{}) bool {
//	t := than.(*SecondaryInt64)
//	if k.key < t.key {
//		return true
//	} else if k.key == t.key {
//		return k.pk < t.pk
//	} else {
//		return false
//	}
//}
//
//type SecondaryString struct {
//	pk   string
//	key  string
//	item *Item
//}
//
//func (k *SecondaryString) Less(than btree.Item, ctx interface{}) bool {
//	t := than.(*SecondaryString)
//	if k.key < t.key {
//		return true
//	} else if k.key == t.key {
//		return k.pk < t.pk
//	} else {
//		return false
//	}
//}

func CreateIntKey(pk string, id int64) string {
	b := make([]byte, len(pk)+8, len(pk)+8)
	binary.LittleEndian.PutUint64(b, uint64(id))
	copy(b[8:], pk)
	return *(*string)(unsafe.Pointer(&b))
}
