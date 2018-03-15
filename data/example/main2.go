package main

import (
	"fmt"
	"unsafe"
)

type IndexItem struct {
	key uintptr
	key2 uintptr
}

func main() {
	//var key uintptr
	//var keyStr = "hi"
	//var key2 uintptr
	//var keyId = 0
	//
	//key = unsafe.Pointer(&key)


	fmt.Println(unsafe.Sizeof(IndexItem{}))
}
