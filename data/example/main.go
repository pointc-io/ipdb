package main

import (
	"fmt"
	"github.com/armon/go-radix"
	radix2 "github.com/gbrlsnchs/radix"
	"time"
	"encoding/binary"
	"unsafe"
	"github.com/pointc-io/sliced/data/btree"
	"github.com/pointc-io/sliced/data/sorted"
)

type Item struct {
	key   string
	value string

	expires int32
	idxs    [4]byte
}

type integer struct {
	a, b, c, d, e, f, g, h byte
}

func (i integer) AsInt() {

}

type s1 struct {
	a1   *s2
	a2   *s2
	key  string
	key2 *string
}

type s2 struct {
	tr      *btree.BTree
	tr2     *btree.BTree
	float32 float64
}

type s3 struct {
	float64 float64
}

type FloatItem struct {
	tr  *btree.BTree
	tr2 *btree.BTree
	key sorted.FloatKey
}

func main() {
	fmt.Println(unsafe.Sizeof(s2{}))
	fmt.Println(unsafe.Sizeof(FloatItem{}))
}

func main8() {
	var s integer

	fmt.Println(unsafe.Sizeof(s1{}))

	str := "hello"
	fmt.Println(unsafe.Sizeof(str))
	strb := *(*[]byte)(unsafe.Pointer(&str))

	//strb[0] = 'C'
	fmt.Println(string(strb))

	v := int64(10)
	s.h = byte(v >> 56)
	s.g = byte(v >> 48)
	s.f = byte(v >> 40)
	s.e = byte(v >> 32)
	s.d = byte(v >> 24)
	s.c = byte(v >> 16)
	s.b = byte(v >> 8)
	s.a = byte(v)

	fmt.Println(v)
	fmt.Println(s)
	fmt.Println(*(*int64)(unsafe.Pointer(&s)))

	fmt.Println(unsafe.Sizeof([16]byte{}))
	fmt.Println(unsafe.Sizeof(integer{}))
	fmt.Println(unsafe.Sizeof(Item{}))

	fmt.Println(unsafe.Sizeof(s2{tr: nil}))
	fmt.Println(unsafe.Sizeof(s3{}))

	fmt.Println(unsafe.Sizeof(""))

	fmt.Println(unsafe.Sizeof([]byte{}))
	fmt.Println(unsafe.Sizeof(float64(0)))
	fmt.Println(unsafe.Sizeof(float32(0)))
}

func main3() {
	t := radix2.New("BenchmarkSingleStatic")

	var buf = make([]byte, 16)
	var now int64
	count := uint64(0)

	for i := 0; i < 1000; i++ {
		n := time.Now().UnixNano() / int64(time.Second)
		if now == n {
			count++
		} else {
			count = 0
		}
		now = n

		binary.BigEndian.PutUint64(buf, uint64(n))
		binary.BigEndian.PutUint64(buf[8:], count)
		key := string(buf)

		t.Add(key, fmt.Sprintf("%d", i))

		//time.Sleep(time.Millisecond)
	}

	t.Print()
	//fmt.Println(n)
	//main2()
}

func main2() {
	// Create a tree
	r := radix.New()

	buf := make([]byte, 16)
	var now int64
	count := uint64(0)

	for i := 0; i < 1000; i++ {
		n := time.Now().UnixNano() / int64(time.Millisecond)
		if now == n {
			count++
		} else {
			count = 0
		}
		now = n

		binary.BigEndian.PutUint64(buf[0:8], uint64(n))
		binary.BigEndian.PutUint64(buf[8:16], count)
		key := string(buf)

		r.Insert(key, fmt.Sprintf("%d", i))
		//fmt.Println(key)

		time.Sleep(time.Millisecond)
	}

	binary.BigEndian.PutUint64(buf, uint64(time.Now().UnixNano()/int64(time.Millisecond)-int64((100))))
	binary.BigEndian.PutUint64(buf[8:], 0)

	//i, ok := r.Get("foo")
	//fmt.Println(ok)
	//fmt.Println(i)

	_, i, ok := r.LongestPrefix(string(buf[0:5]))
	if !ok {
		fmt.Println("Not Found")
	} else {
		fmt.Println("Found: ", i)
	}

	//r.Walk(func(s string, v interface{}) bool {
	//	fmt.Println(s)
	//	return false
	//})

	r.WalkPrefix(string(buf[0:7]), func(key string, v interface{}) bool {
		b := *(*[]byte)(unsafe.Pointer(&key))
		fmt.Println(binary.BigEndian.Uint64(b[0:8]), binary.BigEndian.Uint64(b[8:16]))
		//fmt.Println(key)
		return false
	})

	// Find the longest prefix match
	//m, _, _ := r.LongestPrefix("foozip")
	//if m != "foo" {
	//	panic("should be foo")
	//}
}

func mainB() {
	//tree := btree.New(64, nil)
	//_ = tree
	//
	//b := make([]byte, 4)
	//binary.LittleEndian.PutUint32(b, 10)
	//
	//fmt.Println(binary.LittleEndian.Uint32(b))
	//
	//b = []byte("hi")
	//in := *(*string)(unsafe.Pointer(&b))
	//fmt.Println(in)
	//
	//var item *btree.TreeItem
	//for i := 5; i < 10; i++ {
	//	buf := make([]byte, 4)
	//	binary.LittleEndian.PutUint32(buf, uint32(i))
	//	item = &btree.TreeItem{
	//		Key:   buf,
	//		Value: []byte("Hi"),
	//	}
	//	tree.ReplaceOrInsert(item)
	//}
	//
	//buf := make([]byte, 4)
	//binary.LittleEndian.PutUint32(buf, uint32(8))
	//
	//tree.DescendLessOrEqual(&btree.TreeItem{Key: buf}, func(i btree.Item) bool {
	//	fmt.Println(i)
	//	return true
	//})
	//
	//tree.AscendGreaterOrEqual(&btree.TreeItem{Key: buf}, func(i btree.Item) bool {
	//	fmt.Println(i)
	//	return true
	//})
	//
	//binary.LittleEndian.PutUint32(buf, 1)
	//item = &btree.TreeItem{
	//	Key:   buf,
	//	Value: []byte("Hi"),
	//}
	//tree.ReplaceOrInsert(item)

	//it := tree.ReplaceOrInsert(item)
	//
	//fmt.Println(it)
	//fmt.Println(item)
}
