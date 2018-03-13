package sorted

import (
	"testing"
	"fmt"
	"strconv"
	"github.com/pointc-io/ipdb/codec/gjson"
	"unsafe"
)

func TestNew(t *testing.T) {
	set := New()

	set.Set("1", "bar", 0)

	val, _ := set.Get("1")

	fmt.Println(string(val))
}

func TestPoint(t *testing.T) {
	fmt.Println(unsafe.Sizeof(stringKey{}))
	fmt.Println(unsafe.Sizeof(rectKey{}))
}

func TestSpatial(t *testing.T) {
	db := New()

	//db.CreateJSONStringIndex("last_name", "p:*", "name.last")
	//db.CreateJSONIndex("last_name", "p:*", "name.last")
	db.CreateSpatialIndex("fleet", "fleet:*")
	//db.CreateSpatialIndex("loc", "p:*", "age")

	db.Set("fleet:0:pos", "[-115.567 33.532]", 0)
	db.Set("fleet:1:pos", "[-116.671 35.735]", 0)
	db.Set("fleet:2:pos", "[-113.902 31.234]", 0)

	db.Nearby("fleet", "[-113 33]", func(key *rectKey, val *Item, dist float64) bool {
		fmt.Println(val.Key, val.Value, dist)
		return true
	})
}

func TestJsonSpatial(t *testing.T) {
	db := New()

	//db.CreateJSONStringIndex("last_name", "p:*", "name.last")
	//db.CreateJSONIndex("last_name", "p:*", "name.last")
	db.CreateJSONSpatialIndex("fleet", "p:*", "location")
	//db.CreateSpatialIndex("loc", "p:*", "age")

	db.Set("p:1", `{"name":{"first":"Tom","last":"Johnson"},"age":38, "location":[-115.567 33.532]}`, 0)
	db.Set("p:2", `{"name":{"first":"Janet","last":"Prichard"},"age":47, "location":[-116.671 35.735]}`, 0)
	db.Set("p:3", `{"name":{"first":"Carol","last":"Anderson"},"age":52, "location":[-113.902 31.234]}`, 0)

	db.Nearby("fleet", "[-113 33]", func(key *rectKey, val *Item, dist float64) bool {
		fmt.Println(val.Key, val.Value, dist)
		return true
	})
}

func TestSecondary(t *testing.T) {
	db := New()

	//db.CreateJSONStringIndex("last_name", "p:*", "name.last")
	db.CreateIndexM("last_name_age", "p:*", JSONString("name.last"), JSONNumber("age"))
	db.CreateJSONIndex("last_name", "p:*", "name.last")
	db.CreateJSONNumberIndex("age", "p:*", "age")

	db.Set("p:10", `{"name":{"first":"Don","last":"Johnson"},"age":38}`, 0)
	db.Set("p:1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, 0)
	db.Set("p:2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, 0)
	db.Set("p:3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, 0)
	db.Set("p:4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, 0)
	db.Set("p:40", `{"name":{"first":"Alan","last":30},"age":30}`, 0)
	db.Set("p:400", `{"name":{"first":"Alan","last":28},"age":28}`, 0)
	db.Set("p:4000", `{"name":{"first":"Alan","last":29},"age":29}`, 0)

	//fmt.Println("Order by last name")
	//db.Ascend("last_name", func(key, value string) bool {
	//	fmt.Printf("%s: %s\n", key, value)
	//	return true
	//})
	//
	//fmt.Println("Order by age")
	//db.Descend("age", func(key, value string) bool {
	//	fmt.Printf("%s: %s\n", key, value)
	//	return true
	//})

	fmt.Println("Order by age range 30-50")
	db.Ascend("last_name_age", func(key string, value *Item) bool {
		res := gjson.Get(value.Value, "name.last")
		age := gjson.Get(value.Value, "age")
		fmt.Printf("%s: %s\n", age.Raw, res.Str)
		return true
	})

	fmt.Println("Order by age range 30-50")
	db.AscendRange("age", EncodeFloat("30"), EncodeFloat("50"), func(key string, value *Item) bool {
		res := gjson.Get(value.Value, "name.last")
		age := gjson.Get(value.Value, "age")
		fmt.Printf("%s: %s\n", age.Raw, res.Str)
		return true
	})
}

func TestDesc(t *testing.T) {
	db := New()

	db.CreateIndex("last_name", "p:*")
	db.CreateIndex("age", "p:*")
	db.CreateIndex("age2", "a:*")

	//db.createSecondaryIndex("lastname", "p:*", &jsonKeyFactory{path: "name.last"}, nil)

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
	db.CreateIndex("age2", "a:*")

	fmt.Println("Order by last name")
	db.Descend("last_name", func(key string, value *Item) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})
	fmt.Println("Order by age")
	db.Ascend("age", func(key string, value *Item) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})
	fmt.Println("Order by age range 30-50")
	db.AscendRange("age", `{"age":30}`, `{"age":50}`, func(key string, value *Item) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})

	fmt.Println("Order by age")
	db.AscendRange("age2", `28`, "50", func(key string, value *Item) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})

	fmt.Println("Order by age")
	db.Ascend("age2", func(key string, value *Item) bool {
		fmt.Printf("%s: %s\n", key, value)
		return true
	})
}

func BenchmarkIndexJSON(b *testing.B) {
	db := New()
	db.CreateIndex("last_name", "p:*")
	db.CreateIndex("age", "p:*")
	db.CreateIndex("age2", "a:*")
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
