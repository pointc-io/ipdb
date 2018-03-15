package main

import (
	"testing"
	"github.com/pointc-io/ipdb/index/sorted"
	"github.com/pointc-io/ipdb/index/btree"
	"unsafe"
	"fmt"
)

type myint int64

type Inccer interface {
	inc()
}

type FloatItem struct {
	tr  *btree.BTree
	tr2 *btree.BTree
	key float64
}

type FloatKeyItem struct {
	tr  *btree.BTree
	tr2 *btree.BTree
	key sorted.FloatKey
}

func (i *myint) inc() {
	*i = *i + 1
}

type StringItem string
type StringItem2 struct {
	Value string
}

func BenchmarkCastString(b *testing.B) {
	//buf := []byte("hello")
	str := StringItem("")
	str2 := StringItem("0")

	for k := 0; k < b.N; k++ {
		//_ = StringItem("")
		a := (string)(str) < (string)(str2)
		if a {

		}
	}
}

func BenchmarkCastStringStruct(b *testing.B) {
	//buf := []byte("hello")
	str := StringItem2{""}

	for k := 0; k < b.N; k++ {
		a := str.Value < "0"
		if a {

		}
	}
}

func BenchmarkCastString2(b *testing.B) {
	//buf := []byte("hello")
	str := ""
	str2 := "0"

	for k := 0; k < b.N; k++ {
		a := str < str2
		if a {

		}
	}
}

func BenchmarkUnsafeString(b *testing.B) {
	buf := []byte("hello")
	str := *(*string)(unsafe.Pointer(&buf))
	fmt.Println(str, len(str))
	fmt.Println(str[1:3], len(str[1:3]))

	for k := 0; k < b.N; k++ {
		_ = *(*string)(unsafe.Pointer(&buf))
	}
}

func BenchmarkSafeString(b *testing.B) {
	buf := []byte("hello")

	for k := 0; k < b.N; k++ {
		_ = string(buf)
	}
}

func BenchmarkFloat(b *testing.B) {
	key := float64(1)

	for k := 0; k < b.N; k++ {
		it := FloatItem{
			key: 0,
		}
		if it.key < key {
		}
	}
}

func BenchmarkFloatKey(b *testing.B) {
	key := sorted.FloatKey(1)

	for k := 0; k < b.N; k++ {
		it := FloatKeyItem{
			key: sorted.FloatKey(0),
		}
		if it.key < key {
		}
	}
}

func BenchmarkIntmethod(b *testing.B) {
	i := new(myint)
	incnIntmethod(i, b.N)
}

func BenchmarkInterface(b *testing.B) {
	i := new(myint)
	incnInterface(i, b.N)
}

func BenchmarkTypeSwitch(b *testing.B) {
	i := new(myint)
	incnSwitch(i, b.N)
}

func BenchmarkTypeAssertion(b *testing.B) {
	i := new(myint)
	incnAssertion(i, b.N)
}

func incnIntmethod(i *myint, n int) {
	for k := 0; k < n; k++ {
		i.inc()
	}
}

func incnInterface(any Inccer, n int) {
	for k := 0; k < n; k++ {
		any.inc()
	}
}

func incnSwitch(any Inccer, n int) {
	for k := 0; k < n; k++ {
		switch v := any.(type) {
		case *myint:
			v.inc()
		}
	}
}

func incnAssertion(any Inccer, n int) {
	for k := 0; k < n; k++ {
		if newint, ok := any.(*myint); ok {
			newint.inc()
		}
	}
}
