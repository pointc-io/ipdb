package b

import (
	"testing"
	"strings"
	"fmt"
	"strconv"
	"encoding/binary"
	"unsafe"
)

func TestTreeNew(t *testing.T) {
	tree := TreeNew(func(a, b interface{}) int {
		return strings.Compare(a.(string), b.(string))
	})

	tree.Set("2", "bar")
	tree.Set("2", "bar2")
	tree.Set("3", "bar")
	tree.Set("4", "bar")
	tree.Set("1", "bar")
	tree.Delete("3")
	fmt.Println(tree.Get("1"))

	e, ok := tree.Seek("2")
	if ok {
		for {
			k, v, err := e.Next()
			if err != nil {
				break
			}
			fmt.Println(k, v)
		}

		//k, v, err = e.Next()
		//if err != nil {
		//	t.Fatal(err)
		//}
		//fmt.Println(k, v)
	}
}

func BenchmarkTree_Put(b *testing.B) {
	val := []byte("Some value")
	tree := TreeNew(func(a, b interface{}) int {
		return strings.Compare(a.(string), b.(string))
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		tree.Set(key, val)
	}
}

func BenchmarkGet(b *testing.B) {
	val := []byte("Some value")
	tree := TreeNew(func(a, b interface{}) int {
		return strings.Compare(a.(string), b.(string))
	})

	for i := 0; i < 200000; i++ {
		key := strconv.Itoa(i)
		tree.Set(key, val)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get("100000")
	}
}

func BenchmarkHash_Put(b *testing.B) {
	val := []byte("Some value")
	tree := make(map[string][]byte)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		tree[key] = val
	}
}
