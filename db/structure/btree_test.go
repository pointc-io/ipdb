package btree

import (
	"testing"
	"unsafe"
	"encoding/binary"
)

func BenchmarkBTree_Clone(b *testing.B) {
	by := []byte("HI")
	for i := 0; i < b.N; i++ {
		toString(by)
	}
}

func BenchmarkBTree_Clone2(b *testing.B) {
	by := []byte("HI")
	for i := 0; i < b.N; i++ {
		castString(by)
	}
}

func toString(b []byte) string {
	return string(b)
}

func castString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func BenchmarkPut(b *testing.B) {
	val := []byte("Some value")
	tree := New(64, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(i))
		tree.ReplaceOrInsert(&TreeItem{Key: buf, Value: val})
	}
}

func BenchmarkGet(b *testing.B) {
	val := []byte("Some value")
	tree := New(16, nil)

	for i := 0; i < 200000; i++ {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(i))
		tree.ReplaceOrInsert(&TreeItem{Key: buf, Value: val})
	}

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, 100000)
	item := &TreeItem{Key: buf}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get(item)
	}
}

//func BenchmarkHashPut(b *testing.B) {
//	val := []byte("Some value")
//	tree := make(map[string]*TreeItem)
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		buf := make([]byte, 8)
//		binary.LittleEndian.PutUint64(buf, i)
//		tree[id] = &TreeItem{Key: id, Value: val}
//	}
//}
//
//func BenchmarkHashGet(b *testing.B) {
//	val := []byte("Some value")
//	tree := make(map[int64]*TreeItem)
//
//	for i := 0; i < 200000; i++ {
//		id := int64(i)
//		tree[id] = &TreeItem{Key: id, Value: val}
//	}
//
//	item := int64(70500)
//	var it *TreeItem
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		it = tree[item]
//	}
//
//	if it != nil {
//
//	}
//}

func BenchmarkBTree_Ascend(b *testing.B) {

}
