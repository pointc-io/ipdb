package script

import (
	"testing"

	"github.com/yuin/gopher-lua"
)

func Test(t *testing.T) {
	//L := lua.NewState()
	//defer L.Close()
	//if err := L.DoString(`print("hello")`); err != nil {
	//	panic(err)
	//}

	L := lua.NewState()
	defer L.Close()

	if err := L.DoFile("script.lua"); err != nil {
		panic(err)
	}
}
