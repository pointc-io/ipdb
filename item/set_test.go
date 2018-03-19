package item

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/pointc-io/sliced/codec/gjson"
)

func TestNew(t *testing.T) {
	opts := IncludeString | IncludeInt
	fmt.Println(opts & IncludeFloatAsInt)
}

func TestPoint(t *testing.T) {
	fmt.Println(unsafe.Sizeof(StringKey("")))
	fmt.Println(unsafe.Sizeof(""))
	fmt.Println(unsafe.Sizeof(rectItem{}))
}

func TestComposite2(t *testing.T) {
	db := NewSortedSet()

	//db.CreateJSONStringIndex("last_name", "p:*", "name.last")
	//db.CreateJSONIndex("last_name", "p:*", "name.last")
	//db.CreateJSONSpatialIndex("fleet", "p:*", "location")
	//db.CreateSpatialIndex("loc", "p:*", "age")
	db.CreateIndex(
		"last_name_age",
		"*",
		JSONComposite(
			JSONIndexer("age", IncludeInt|IncludeFloat|IncludeFloatAsInt),
			JSONIndexer("name.last", IncludeString|CaseInsensitive)))

	db.Set(StringKey("p:1"), `{"name":{"first":"Tom","last":"Johnson"},"age":38, "location":[-115.567 33.532]}`, 0)
	db.Set(StringKey("p:10"), `{"name":{"first":"Tom","last":"Alpha"},"age":38, "location":[-115.567 33.532]}`, 0)
	db.Set(StringKey("p:101"), `{"name":{"first":"Tom","last":"Beta"},"age":38, "location":[-115.567 33.532]}`, 0)
	db.Set(StringKey("p:1011"), `{"name":{"first":"Tom","last":"beta"},"age":38, "location":[-115.567 33.532]}`, 0)
	db.Set(StringKey("p:2"), `{"name":{"first":"Janet","last":"Prichard"},"age":47, "location":[-116.671 35.735]}`, 0)
	db.Set(StringKey("p:3"), `{"name":{"first":"Carol","last":"Anderson"},"age":52, "location":[-113.902 31.234]}`, 0)

	db.Ascend("last_name_age", func(key IndexItem) bool {
		//db.AscendRange("age", &floatItem{key: 30}, &floatItem{key: 51}, func(key Value) bool {
		res := gjson.Get(key.Value().Value, "name.last")
		age := gjson.Get(key.Value().Value, "age")
		fmt.Printf("%s: %s\n", age.Raw, res.Raw)
		return true
	})
}

func TestSpatial(t *testing.T) {
	db := NewSortedSet()

	//db.CreateJSONStringIndex("last_name", "p:*", "name.last")
	//db.CreateJSONIndex("last_name", "p:*", "name.last")
	db.CreateSpatialIndex("fleet", "fleet:*", SpatialIndexer())
	//db.CreateSpatialIndex("loc", "p:*", "age")

	db.Set(StringKey("fleet:0:pos"), "[-115.567 33.532]", 0)
	db.Set(StringKey("fleet:1:pos"), "[-116.671 35.735]", 0)
	db.Set(StringKey("fleet:2:pos"), "[-113.902 31.234]", 0)

	db.Nearby("fleet", "[-113 33]", func(key *rectItem, val *ValueItem, dist float64) bool {
		fmt.Println(val.Key, val.Value, dist)
		return true
	})
}

func TestIndexer(t *testing.T) {
	db := NewSortedSet()

	//indexer := NewIndexer("name.last", data.String, false, JSONProjector("name.last"))
	//db.CreateIndex("last_name", "p:*", indexer)

	db.CreateSpatialIndex("fleet", "p:*", JSONSpatialIndexer("location"))

	//db.CreateJSONStringIndex("last_name", "p:*", "name.last")
	//db.CreateJSONIndex("last_name", "p:*", "name.last")
	//db.CreateJSONSpatialIndex("fleet", "p:*", "location")
	//db.CreateSpatialIndex("loc", "p:*", "age")

	db.Set(StringKey("p:1"), `{"name":{"first":"Tom","last":"Johnson"},"age":38, "location":[-115.567 33.532]}`, 0)
	db.Set(StringKey("p:2"), `{"name":{"first":"Janet","last":"Prichard"},"age":47, "location":[-116.671 35.735]}`, 0)
	db.Set(StringKey("p:3"), `{"name":{"first":"Carol","last":"Anderson"},"age":52, "location":[-113.902 31.234]}`, 0)

	db.Nearby("fleet", "[-113 33]", func(key *rectItem, value *ValueItem, dist float64) bool {
		fmt.Println(value.Key, value.Value, dist)
		return true
	})
}

func TestJsonSpatial(t *testing.T) {
	db := NewSortedSet()

	//db.CreateJSONStringIndex("last_name", "p:*", "name.last")
	//db.CreateJSONIndex("last_name", "p:*", "name.last")
	//db.CreateJSONSpatialIndex("fleet", "p:*", "location")
	//db.CreateSpatialIndex("loc", "p:*", "age")

	db.Set(StringKey("p:1"), `("name":("first":"Tom","last":"Johnson"),"age":38, "location":[-115.567 33.532])`, 0)
	db.Set(StringKey("p:2"), `("name":("first":"Janet","last":"Prichard"),"age":47, "location":[-116.671 35.735])`, 0)
	db.Set(StringKey("p:3"), `("name":("first":"Carol","last":"Anderson"),"age":52, "location":[-113.902 31.234])`, 0)

	//db.Nearby("fleet", "[-113 33]", func(key *rectKey, val *Value, dist float64) bool (
	//	fmt.Println(val.K, val.Value, dist)
	//	return true
	//))
}

func TestRect(t *testing.T) {
	str := EncodeFloat64("30")
	fmt.Println(StrAsFloat64(str))
	fmt.Println(unsafe.Sizeof(FloatKey(0)))
	fmt.Println(unsafe.Sizeof(NilKey{}))
}

func TestSecondary(t *testing.T) {
	//key := &FloatItem{}
	//fmt.Println(unsafe.Offsetof(key.K))

	db := NewSortedSet()

	//db.CreateJSONStringIndex("last_name", "p:*", "name.last")
	//db.CreateIndexM("last_name_age", "p:*", JSONString("name.last"), JSONNumber("age"))
	//db.CreateIndexM("last_name_age", "p:*", JSONNumber("age"), JSONString("name.last"))
	//db.CreateJSONIndex("last_name", "p:*", "name.last")

	//db.CreateJSONNumberIndex("age", "p:*", "age")
	db.CreateIndex(
		"last_name",
		"*",
		JSONIndexer("name.last", IncludeString))
	db.CreateIndex(
		"age",
		"*",
		JSONIndexer("age", IncludeInt|IncludeFloat))

	db.Set(StringKey("p:10"), `{"name":{"first":"Don","last":"Johnson"},"age":38}`, 0)
	db.Set(StringKey("p:1"), `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, 0)
	db.Set(StringKey("p:2"), `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, 0)
	db.Set(StringKey("p:3"), `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, 0)
	db.Set(StringKey("p:4"), `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, 0)
	db.Set(StringKey("p:40"), `{"name":{"first":"Alan","last":30},"age":30}`, 0)
	db.Set(StringKey("p:400"), `{"name":{"first":"Alan","last":28},"age":28}`, 0)
	db.Set(StringKey("p:4000"), `{"name":{"first":"Alan","last":29},"age":29}`, 0)
	db.Set(IntKey(4000), `{"name":{"first":"Alan","last":29},"age":29}`, 0)

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
	fmt.Println("Table Scan")
	db.AscendPrimary(func(item *ValueItem) bool {
		res := gjson.Get(item.Value, "name.last")
		age := gjson.Get(item.Value, "age")
		fmt.Printf("%s %s: %s\n", item.Key, age.Raw, res.Raw)
		return true
	})

	fmt.Println()
	fmt.Println("Order by age range 30-50")
	db.Descend("last_name", func(key IndexItem) bool {
		res := gjson.Get(key.Value().Value, "name.last")
		age := gjson.Get(key.Value().Value, "age")
		fmt.Printf("%s: %s\n", age.Raw, res.Raw)
		return true
	})

	fmt.Println()
	fmt.Println("Order by age range 30-50")
	db.Ascend("age", func(key IndexItem) bool {
		//db.AscendRange("age", &floatItem{key: 30}, &floatItem{key: 51}, func(key Value) bool {
		res := gjson.Get(key.Value().Value, "name.last")
		age := gjson.Get(key.Value().Value, "age")
		fmt.Printf("%s: %s\n", age.Raw, res.Raw)
		return true
	})
	//db.AscendRange("age", FloatKey(30), FloatKey(52), func(key Value) bool {
	//	//db.AscendRange("age", &floatItem{key: 30}, &floatItem{key: 51}, func(key Value) bool {
	//	res := gjson.SliceForKey(key.Value().Value, "name.last")
	//	age := gjson.SliceForKey(key.Value().Value, "age")
	//	fmt.Printf("%s: %s\n", age.Raw, res.Raw)
	//	return true
	//})
}

func TestDesc(t *testing.T) {
	db := NewSortedSet()

	//db.createSecondaryIndex("lastname", "p:*", &jsonKeyFactory{path: "name.last"}, nil)

	//db.SortedSet("p:10", `{"name":{"first":"Don","last":"Johnson"},"age":38}`, 0)
	//db.SortedSet("p:1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, 0)
	//db.SortedSet("p:2", `{"name":{"first":"Janet","last":"Prichard"},"age":47}`, 0)
	//db.SortedSet("p:3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, 0)
	//db.SortedSet("p:4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, 0)
	//
	//db.SortedSet("a:5", `9`, 0)
	//db.SortedSet("a:6", `47`, 0)
	//db.SortedSet("a:10", `47`, 0)
	//db.Delete("a:10")
	//db.SortedSet("a:7", `52`, 0)
	//db.SortedSet("a:8", `28`, 0)
	//db.SortedSet("a:9", `100`, 0)
	//db.SortedSet(StringKey{"a:9"}, `test`, 0)

	//db.Delete("a:9")
	item := db.get(StringKey(""))
	fmt.Println(item)

	db.DropIndex("age2")

	fmt.Println("Order by last name")
	db.Descend("last_name", func(key IndexItem) bool {
		fmt.Printf("%s: %s\n", key, key.Value().Value)
		return true
	})
	fmt.Println("Order by age")
	db.Ascend("age", func(key IndexItem) bool {
		fmt.Printf("%s: %s\n", key, key.Value().Value)
		return true
	})
	fmt.Println("Order by age range 30-50")
	//db.AscendRange("age", `{"age":30}`, `{"age":50}`, func(key Value) bool {
	//	fmt.Printf("%s: %s\n", key, key.Value().Value)
	//	return true
	//})
	//
	//fmt.Println("Order by age")
	//db.AscendRange("age2", `28`, "50", func(key Value) bool {
	//	fmt.Printf("%s: %s\n", key, key.Value().Value)
	//	return true
	//})

	fmt.Println("Order by age")
	db.Ascend("age2", func(key IndexItem) bool {
		fmt.Printf("%s: %s\n", key, key.Value().Value)
		return true
	})
}

func BenchmarkIndexJSON(b *testing.B) {
}
