package sorted

import (
	"testing"
	"fmt"
	"strconv"
)

func TestNew(t *testing.T) {
	set := New()

	set.Set("1", "bar", 0)

	val, _ := set.Get("1")

	fmt.Println(string(val))
}

func TestSecondary(t *testing.T) {
	db := New()

	db.CreateJsonIndex("last_name", "p:*", "name.last")

	db.Set("p:10", `{"name":{"first":"Don","last":"Johnson"},"age":38}`, 0)
	db.Set("p:1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, 0)
	db.Set("p:2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, 0)
	db.Set("p:3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, 0)
	db.Set("p:4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, 0)
	db.Set("p:5", `0`, 0)


	fmt.Println("Order by last name")
	db.Ascend("last_name", func(key, value string) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})
}

func TestDesc(t *testing.T) {
	db := New()

	db.CreateIndex("last_name", "p:*", IndexJSON("name.last"))
	db.CreateIndex("age", "p:*", IndexJSON("age"))
	db.CreateIndex("age2", "a:*", IndexInt)

	db.createSecondaryIndex("lastname", "p:*", &jsonKeyFactory{path: "name.last"}, nil)

	db.Set("p:10", `{"name":{"first":"Don","last":"Johnson"},"age":38}`, 0)
	db.Set("p:1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, 0)
	db.Set("p:2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, 0)
	db.Set("p:3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, 0)
	db.Set("p:4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, 0)

	db.Set("a:5", `9`, 0)
	db.Set("a:6", `47`, 0)
	db.Set("a:10", `47`, 0)
	db.Delete("a:10")
	db.Set("a:7", `52`, 0)
	db.Set("a:8", `28`, 0)
	db.Set("a:9", `100`, 0)
	db.Set("a:9", `test`, 0)

	//db.Delete("a:9")
	item := db.get("")
	fmt.Println(item)

	db.DropIndex("age2")
	db.CreateIndex("age2", "a:*", IndexInt)

	fmt.Println("Order by last name")
	db.Descend("last_name", func(key, value string) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})
	fmt.Println("Order by age")
	db.Ascend("age", func(key, value string) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})
	fmt.Println("Order by age range 30-50")
	db.AscendRange("age", `{"age":30}`, `{"age":50}`, func(key, value string) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})

	fmt.Println("Order by age")
	db.AscendRange("age2", `28`, "50", func(key, value string) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})

	fmt.Println("Order by age")
	db.Ascend("age2", func(key, value string) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})
}

func BenchmarkIndexJSON(b *testing.B) {
	db := New()
	db.CreateIndex("last_name", "p:*", IndexJSON("name.last"))
	db.CreateIndex("age", "p:*", IndexJSON("age"))
	db.CreateIndex("age2", "a:*", IndexInt)
}

func BenchmarkTx_Set(b *testing.B) {
	set := New()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set.Set(strconv.Itoa(i), "bar", 0)
		//tx, err := set.Begin(true)
		//if err != nil {
		//	b.Fatal(err)
		//}
		//tx.Set(strconv.Itoa(i), "bar", 0)
		//tx.Commit()
	}
}
