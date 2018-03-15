package main

import (
	"github.com/armon/go-radix"
	"fmt"
	"github.com/pointc-io/ipdb/data/sorted"
)

func main() {

	val := []byte("Some value")
	tree := radix.New()

	for i := 0; i < 10000; i++ {
		key := sorted.IntToString(i)
		//key := strconv.Itoa(i)
		tree.Insert(key, val)
	}

	//m2, v2, _ := tree.LongestPrefix("50")
	//fmt.Println(m2)

	tree.Walk(func(s string, v interface{}) bool {
		fmt.Println(s)
		fmt.Println(fmt.Sprintf("%s", v))
		return false
	})

	m, v, ok := tree.Minimum()
	if !ok {
		fmt.Println("Not OK")
	} else {
		fmt.Println(m)
		fmt.Println(fmt.Sprintf("%s", v))

	}

	//tree := art.New()
	//
	//tree.Insert(art.Key("key:10"), "Nice to meet you, I'm Value")
	//tree.Insert(art.Key("key:2"), "Nice to meet you, I'm Value")
	//tree.Insert(art.Key("key:9"), "Nice to meet you, I'm Value")
	//
	//
	//value, found := tree.Search(art.Key("key"))
	//if found {
	//	fmt.Printf("Search value=%v\n", value)
	//}
	//
	//tree.ForEachPrefix(art.Key("key:"), func(node art.Node) bool {
	//	fmt.Printf("Callback key=%s value=%v\n", node.Key(), node.Value())
	//	return true
	//})
	//
	//for it := tree.Iterator(); it.HasNext(); {
	//	value, _ := it.Next()
	//	fmt.Printf("Iterator key=%s value=%v\n", value.Key(), value.Value())
	//}
}
