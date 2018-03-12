package main

import (
	"fmt"

	"github.com/tidwall/buntdb"
	"encoding/binary"
	"unsafe"
)

func main() {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, 120)

	b2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b2, 65000000)

	i := *(*int32)(unsafe.Pointer(&b[0]))
	fmt.Println(i)
	fmt.Println(b)
	fmt.Println(b2)

	db, _ := buntdb.Open(":memory:")
	db.Update(func(tx *buntdb.Tx) error {
		tx.CreateIndex("last_name", "p:*", buntdb.IndexJSON("name.last"))
		tx.CreateIndex("age", "p:*", buntdb.IndexJSON("age"))
		return tx.CreateIndex("age2", "a:*", buntdb.IndexInt)
	})

	db.Update(func(tx *buntdb.Tx) error {
		tx.Set("p:1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, nil)
		tx.Set("p:2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, nil)
		tx.Set("p:3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, nil)
		tx.Set("p:4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, nil)

		tx.Set("a:5", `9`, nil)
		tx.Set("a:6", `47`, nil)
		tx.Set("a:7", `52`, nil)
		tx.Set("a:8", `28`, nil)
		tx.Set("a:9", `100`, nil)
		return nil
	})
	db.View(func(tx *buntdb.Tx) error {
		fmt.Println("Order by last name")
		tx.Ascend("last_name", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age")
		tx.Ascend("age", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age range 30-50")
		tx.AscendRange("age", `{"age":30}`, `{"age":50}`, func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})

		fmt.Println("Order by age")
		tx.AscendRange("age2", `28`, "50", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})
}
