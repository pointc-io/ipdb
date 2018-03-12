package main

import (
	"testing"
	"github.com/vmihailenco/msgpack"
	"github.com/pointc-io/ipdb/codec/gjson"
	"fmt"
	"strconv"
)

func BenchmarkForEachLine(b *testing.B) {
	buf, err := msgpack.Marshal(map[string]interface{}{
		"id": 1, "name": map[string]interface{}{"first": "Mike", "last": "Wilkins"}, "attrs": map[string]interface{}{"phone": 12345},
	})
	if err != nil {
		panic(err)
	}

	l := len(buf)
	//buffer := bytes.NewBuffer(buf)
	rd := &reader{buf: buf}
	//dec := msgpack.NewDecoder(bytes.NewBuffer(buf))
	dec := msgpack.NewDecoder(rd)

	for i := 0; i < b.N; i++ {
		values, err := dec.Query("attrs.phone")
		if err != nil {
			panic(err)
		}

		_ = values
		//fmt.Println("phones are", values)

		rd.buf = rd.buf[0:l]
		dec.Reset(rd)
	}

	//fmt.Println("phones are", values)
}

func TestGJSON(b *testing.T) {
	json := "{\"id\": 1, \"name\":{\"first\":\"Mike\", \"last\":\"Wilkins\"}, \"attrs\":{\"phone\": 12345}}"

	fmt.Println(Parse("0443"))
	fmt.Println(gjson.Parse("-12.033").Num)
	fmt.Println(gjson.Parse("-12").Num)
	fmt.Println(gjson.Parse("true").Type)
	fmt.Println(gjson.Parse("[32.00]").Type)
	fmt.Println(gjson.Get(json, "name.last"))
	fmt.Println(gjson.Get(json, "name.first"))
	fmt.Println(gjson.Get(json, "name.first").Type)
	fmt.Println(gjson.Get(json, "attrs.phone"))
	fmt.Println(gjson.Get(json, "attrs.phone").Type)
	fmt.Println(gjson.Get("[32.00]", "[0]").Type)
}

func BenchmarkGJSONParse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		gjson.Parse("0445")
	}
	//fmt.Println(gjson.Parse("0").Type)
}

func BenchmarkGJSONParseFast(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Parse("0445")
	}
	//fmt.Println(gjson.Parse("0").Type)
}

func BenchmarkAtoi(b *testing.B) {
	for i := 0; i < b.N; i++ {
		strconv.Atoi("0")
	}
	//fmt.Println(gjson.Parse("0").Type)
}

func BenchmarkGJSON(b *testing.B) {
	json := "{\"id\": 1, \"name\":{\"first\":\"Mike\", \"last\":\"Wilkins\"}, \"attrs\":{\"phone\": 12345}}"

	fmt.Println(gjson.Get(json, "name.first"))
	fmt.Println(gjson.Get(json, "name.first"))
	fmt.Println(gjson.Get(json, "name.first").Type)
	fmt.Println(gjson.Get(json, "attrs.phone"))
	fmt.Println(gjson.Get(json, "attrs.phone").Type)

	for i := 0; i < b.N; i++ {
		//result := gjson.Get(json, "attrs.phone")
		gjson.Get(json, "id")

		//fmt.Println(result)
	}

	//fmt.Println("phones are", values)
}

type reader struct {
	off int
	buf []byte
}

func (r *reader) Read(p []byte) (n int, err error) {
	l := len(p)
	if l < len(r.buf) {
		copy(p, r.buf[0:l])
		return l, nil
	} else {
		copy(p, r.buf)
		return l, nil
	}
}

// Parse parses the json and returns a result.
//
// This function expects that the json is well-formed, and does not validate.
// Invalid json will not panic, but it may return back unexpected results.
// If you are consuming JSON from an unpredictable source then you may want to
// use the Valid function first.
func Parse(json string) (gjson.Type, int64) {
	var value gjson.Type
	for i := 0; i < len(json); i++ {
		if json[i] == '{' || json[i] == '[' {
			value = gjson.JSON
			//value.Raw = json[i:] // just take the entire raw
			break
		}
		if json[i] <= ' ' {
			continue
		}
		switch json[i] {
		default:
			if (json[i] >= '0' && json[i] <= '9') || json[i] == '-' {
				value = gjson.Number
				//b := *(*[]byte)(unsafe.Pointer(&json))
				//ret := *(*string)(unsafe.Pointer(&b[i]))
				//strconv.ParseFloat(json, 64)
				iii, _ := strconv.ParseInt(json, 10, 64)
				return value, int64(iii)
				//value.Raw, value.Num = tonum(json[i:])
			} else {
				return gjson.Null, 0
			}
		case 'n':
			//value.Type = Null
			return gjson.Null, 0
			//value.Raw = tolit(json[i:])
		case 't':
			return gjson.True, 1
			//value.Type = True
			//value.Raw = tolit(json[i:])
		case 'f':
			//value.Type = False
			return gjson.False, 0
			//value.Raw = tolit(json[i:])
		case '"':
			//value.Type = String
			//value.Raw, value.Str = tostr(json[i:])
			return gjson.String, 0
		}
		break
	}
	return value, 0
}
