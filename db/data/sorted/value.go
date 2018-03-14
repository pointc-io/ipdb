package sorted

import (
	"time"
	"github.com/pointc-io/ipdb/db/data"
)

//
type Value interface {
	Type() data.DataType
}

type NilValue struct{}

func (s NilValue) Type() data.DataType {
	return data.Nil
}

//
//
type StringValue string

func (s *StringValue) Type() data.DataType {
	return data.String
}

//
//
type JSONValue struct {
	Value []byte
}

func (s *JSONValue) Type() data.DataType {
	return data.JSON
}

type MsgPackValue struct {
	Value []byte
}

func (s *MsgPackValue) Type() data.DataType {
	return data.MsgPack
}

//
//
type IntValue int64

func (s IntValue) Type() data.DataType {
	return data.Int
}

//
//
//
type FloatValue float64

func (s FloatValue) Type() data.DataType {
	return data.Float
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
