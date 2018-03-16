package item

import (
	"time"

	"github.com/pointc-io/sliced"
)

type DocumentOpts int

const (
	GZIP DocumentOpts = 1 << iota
	LZ4
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

type CompressedValue struct {
	Size  int // Number of bytes the uncompressed value is
	Value string
}

func (s *CompressedValue) Type() sliced.DataType {
	return sliced.String
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
