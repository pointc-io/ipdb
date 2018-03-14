package main

import (
	"testing"
	"github.com/pointc-io/ipdb/db/data/sorted"
	"github.com/pointc-io/ipdb/db/data/btree"
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
	key := sorted.FloatKey{1}

	for k := 0; k < b.N; k++ {
		it := FloatKeyItem{
			key: sorted.FloatKey{0},
		}
		if it.key.Value < key.Value {
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