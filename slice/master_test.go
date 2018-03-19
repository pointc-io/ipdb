package slice

import (
	"testing"
	"fmt"
	"path/filepath"

	"github.com/pointc-io/sliced"
	"github.com/pointc-io/sliced/db/buntdb"
)

var db *SliceMaster

func init() {
	sliced.Logger = sliced.CLILogger()
}

func runFile(t *testing.T, f func(db *SliceMaster)) {
	run(t, filepath.Join(sliced.Path, "data"), f)
}

func runMemory(t *testing.T, f func(db *SliceMaster)) {
	run(t, ":memory:", f)
}

func run(t *testing.T, path string, f func(db *SliceMaster)) {
	db = NewMaster("", path)
	err := db.Start()
	if err != nil {
		t.Fatal(err)
	}

	f(db)
	err = db.Stop()
	if err != nil {
		t.Fatal(err)
	}
	db.Wait()
}

func TestDB(t *testing.T) {
	runMemory(t, func(db *SliceMaster) {

	})
}

func TestNewDB(t *testing.T) {
	db := NewMaster("", ":memory:")
	err := db.Start()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(db.SliceForKey("somekey{0110}withredishash_part").id)
	fmt.Println(db.SliceForKey("a_a{0110}a_a").id)
	fmt.Println(db.SliceForKey("0110").id)

	db.SliceForKey("somekey{0110}withredishash_part").db.Update(func(tx *buntdb.Tx) error {
		return nil
	})
}

//func TestNewPartition(t *testing.T) {
//	p := NewPartition(0, ":memory:")
//	err := p.Start()
//
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	key := "item:001"
//	value := "{\"name\":\"ACME\"}"
//
//	// Key-Value
//	p.db.Update(func(tx *buntdb.Tx) error {
//		tx.SortedSet(key, value, nil)
//		return nil
//	})
//	p.db.View(func(tx *buntdb.Tx) error {
//		val, err := tx.SliceForKey(key)
//
//		if err != nil {
//			t.Error(err)
//			return err
//		}
//
//		if val != value {
//			t.Error("Unexpected value")
//		}
//
//		return nil
//	})
//
//	// Spatial indexing
//	p.db.Update(func(tx *buntdb.Tx) error {
//		tx.SortedSet("fleet:0:pos", "[-115.567 33.532]", nil)
//		tx.SortedSet("fleet:1:pos", "[-116.671 35.735]", nil)
//		tx.SortedSet("fleet:2:pos", "[-113.902 31.234]", nil)
//		tx.CreateSpatialIndex("fleet", "fleet:*:pos", buntdb.IndexRect)
//		return nil
//	})
//	p.db.View(func(tx *buntdb.Tx) error {
//		tx.Nearby("fleet", "[-113 33]", func(key, val string, dist float64) bool {
//			fmt.Printf("Key: %s -> Value: %s -> Distance: %f\n", key, val, dist)
//			return true
//		})
//		return nil
//	})
//
//	p.db.Update(func(tx *buntdb.Tx) error {
//		tx.SortedSet("p:1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, nil)
//		tx.SortedSet("p:2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, nil)
//		tx.SortedSet("p:3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, nil)
//		tx.SortedSet("p:4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, nil)
//		tx.SortedSet("p:5", `{"name":{"first":"Sam","last":"Anderson"},"age":51}`, nil)
//		tx.SortedSet("p:6", `{"name":{"first":"Melinda","last":"Prichard"},"age":44}`, nil)
//		tx.CreateIndex("last_name_age", "p:*", buntdb.IndexJSON("name.last"), buntdb.IndexJSON("age"))
//		tx.CreateIndex("last_name", "p:*", buntdb.IndexJSON("name.last"))
//		tx.CreateIndex("age", "p:*", buntdb.IndexJSON("age"))
//		return nil
//	})
//	p.db.View(func(tx *buntdb.Tx) error {
//		fmt.Println()
//		fmt.Println("last_name_age ASC")
//		tx.Ascend("last_name_age", func(key, value string) bool {
//			fmt.Printf("%s: %s\n", key, value)
//			return true
//		})
//		fmt.Println()
//		fmt.Println("last_name DESC")
//		tx.Descend("last_name", func(key, value string) bool {
//			fmt.Printf("LastName: %s\n", gjson.SliceForKey(value, "name.last"))
//			//fmt.Printf("%s: %s\n", key, value)
//
//			return true
//		})
//		fmt.Println()
//		fmt.Println("last_name ASC")
//		tx.Ascend("last_name", func(key, value string) bool {
//			//fmt.Printf("LastName: %s\n", gjson.SliceForKey(value, "name.last"))
//			fmt.Printf("%s: %s\n", key, value)
//			return true
//		})
//		fmt.Println()
//		fmt.Println("age ASC")
//		tx.Ascend("age", func(key, value string) bool {
//			fmt.Printf("%s: %s\n", key, value)
//			return true
//		})
//		return nil
//	})
//
//	p.Stop()
//	p.Wait()
//}
