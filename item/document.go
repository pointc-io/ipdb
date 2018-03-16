package item

import (
	"unsafe"

	"github.com/pointc-io/sliced"
	"github.com/pointc-io/sliced/codec/gjson"
	"github.com/pointc-io/sliced/codec/sjson"
)

//
//
type Document interface {
	Get(path string)

	Set(path string, value interface{})
}

//
type DocumentValue struct {
	Flags int
	Size  int
	Value []byte
}


//
type JSONValue struct {
	DocumentValue
}

func (s JSONValue) Type() sliced.DataType {
	return sliced.JSON
}

func (j JSONValue) Get(path string) {
	result := gjson.Get(*(*string)(unsafe.Pointer(&j.Value)), path)
	_ = result
}

func (j JSONValue) Set(path string, value interface{}) {
	sjson.SetBytes(j.Value, path, value)
}

//
//
type MsgPackValue struct {
	DocumentValue
}

func (s *MsgPackValue) Type() sliced.DataType {
	return sliced.MsgPack
}