package item

import (
	"time"
	"github.com/pointc-io/sliced"
)

//
type Value interface {
	Type() sliced.DataType
}

type NilValue struct{}

func (s NilValue) Type() sliced.DataType {
	return sliced.Nil
}

//
//
type StringValue string

func (s *StringValue) Type() sliced.DataType {
	return sliced.String
}

//
//
type JSONValue struct {
	Value []byte
}

func (s *JSONValue) Type() sliced.DataType {
	return sliced.JSON
}

type MsgPackValue struct {
	Value []byte
}

func (s *MsgPackValue) Type() sliced.DataType {
	return sliced.MsgPack
}

//
//
type IntValue int64

func (s IntValue) Type() sliced.DataType {
	return sliced.Int
}

//
//
//
type FloatValue float64

func (s FloatValue) Type() sliced.DataType {
	return sliced.Float
}

//
//
//
type TimeValue time.Time

//
//
//
type HashValue struct {
	Value map[string]Value
}

//
//
//
type ListValue struct {
	Value []byte
}

//type RectValue grect.Rect
