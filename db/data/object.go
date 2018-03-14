package data

import (
	"errors"
)

var (
	ErrInvalidType = errors.New("expected string")
)

type ObjectType uint8

const (
	JSON    ObjectType = 0
	MsgPack ObjectType = 1
	Num     ObjectType = 2
	String  ObjectType = 3
	Rect    ObjectType = 4
	Nil     ObjectType = 5
)

//type ObjectStructure struct {
//	key string // Key
//}
//
//func (b *ObjectStructure) ObjectType() ObjectType {
//	return -1
//}
//
//// Current committed log index
//func (b *ObjectStructure) Index() uint64 {
//	return 0
//}
//
//func (b *ObjectStructure) Type() StructureType {
//	return Blob
//}
//
//// Handle command
//func (b *ObjectStructure) Process(ctx *evred.CommandContext) evred.Command {
//	return nil
//}
//
//// Snapshot RESP to AOF
//func (b *ObjectStructure) Snapshot(writer io.Writer) error {
//	return nil
//}
//
//// Restore from RESP AOF
//func (b *ObjectStructure) Restore(reader io.Reader) error {
//	return nil
//}
//
////
////
////
//type JSONStructure struct {
//	ObjectStructure
//
//	value []byte
//}
//
//func (b *JSONStructure) ObjectType() ObjectType {
//	return JSON
//}
//
//func (b *JSONStructure) Get(path string) gjson.Result {
//	return gjson.Get(*(*string)(unsafe.Pointer(&b.value)), path)
//}
//
//func (b *JSONStructure) Set(path string, value interface{}) ([]byte, error) {
//	return sjson.SetBytes(b.value, path, value)
//}
//
//func (b *JSONStructure) Incr(path string, by int64) (int64, error) {
//	result := gjson.GetBytes(b.value, path)
//
//	return 0, nil
//}
//
//// Snapshot RESP to AOF
//func (b *JSONStructure) Snapshot(writer io.Writer) error {
//	return nil
//}
//
//// Restore from RESP AOF
//func (b *JSONStructure) Restore(reader io.Reader) error {
//	return nil
//}
//
////
////
////
//type StringStructure struct {
//	ObjectStructure
//
//	data string
//}
//
//func (b *StringStructure) ObjectType() ObjectType {
//	return JSON
//}
//
//func (b *StringStructure) Value() (string, error) {
//	return b.data, nil
//}
//
//// Snapshot RESP to AOF
//func (b *StringStructure) Snapshot(writer io.Writer) error {
//	return nil
//}
//
//// Restore from RESP AOF
//func (b *StringStructure) Restore(reader io.Reader) error {
//	return nil
//}
//
////
////
////
//type IntStructure struct {
//	ObjectStructure
//
//	value int64
//}
//
//func (b *IntStructure) ObjectType() ObjectType {
//	return JSON
//}
//
//func (b *IntStructure) Value() int64 {
//	return b.value
//}
//
//// Snapshot RESP to AOF
//func (b *IntStructure) Snapshot(writer io.Writer) error {
//	return nil
//}
//
//// Restore from RESP AOF
//func (b *IntStructure) Restore(reader io.Reader) error {
//	return nil
//}
//
////
////
////
//type FloatStructure struct {
//	ObjectStructure
//
//	value float64
//}
//
//func (b *FloatStructure) ObjectType() ObjectType {
//	return JSON
//}
//
//func (b *FloatStructure) Value() float64 {
//	return b.value
//}
//
//// Snapshot RESP to AOF
//func (b *FloatStructure) Snapshot(writer io.Writer) error {
//	return nil
//}
//
//// Restore from RESP AOF
//func (b *FloatStructure) Restore(reader io.Reader) error {
//	return nil
//}
//
//// Less tests whether the current item is less than the given argument.
////
//// This must provide a strict weak ordering.
//// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
//// hold one of either a or b in the tree).
////
//// There is a user-defined ctx argument that is equal to the ctx value which
//// is set at time of the btree contruction.
//func (b *FloatStructure) Less(than btree.Item, ctx interface{}) bool {
//	return b.value < than.(*FloatStructure).value
//}
//
////
////
////
//type RectStructure struct {
//	ObjectStructure
//
//	value grect.Rect
//}
//
//func (b *RectStructure) ObjectType() ObjectType {
//	return JSON
//}
//
//func (b *RectStructure) Value() float64 {
//	return 0
//}
//
//// Snapshot RESP to AOF
//func (b *RectStructure) Snapshot(writer io.Writer) error {
//	return nil
//}
//
//// Restore from RESP AOF
//func (b *RectStructure) Restore(reader io.Reader) error {
//	return nil
//}
//
//// Less tests whether the current item is less than the given argument.
////
//// This must provide a strict weak ordering.
//// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
//// hold one of either a or b in the tree).
////
//// There is a user-defined ctx argument that is equal to the ctx value which
//// is set at time of the btree contruction.
//func (b *RectStructure) Less(than btree.Item, ctx interface{}) bool {
//	r := than.(*RectStructure).value
//	return false
//}
//
////func ParseType(value string) ObjectType {
////	if len(value) == 0 {
////		return Nil
////	}
////	b := *(*[]byte)(unsafe.Pointer(&value))
////	if b[0] == '{' {
////
////	}
////	switch b[0] {
////	case '{':
////		return JSON
////	case '-':
////	case '+':
////	case '0':
////	case '1':
////	case '2':
////	default:
////
////	}
////
////	return String
////}
