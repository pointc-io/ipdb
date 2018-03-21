package sliced

import (
	"errors"
)

var (
	ErrInvalidType = errors.New("expected string")
)

type DataType uint8

const (
	Nil    DataType = 0 // Keyable
	String DataType = 1 // Keyable
	Int    DataType = 2 // Keyable
	Float  DataType = 3 // Keyable
	Bool   DataType = 4 // Keyable
	Rect   DataType = 5 // Keyable
	Time   DataType = 6 // Keyable

	// Message Formats
	JSON    DataType = 8
	MsgPack DataType = 9

	Any DataType = 0

	Composite DataType = 99

	// Data Structures

)
