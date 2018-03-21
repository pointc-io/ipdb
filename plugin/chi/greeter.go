package main

import (
	"fmt"

	"github.com/pointc-io/sliced"
)

type greeting string

func (g greeting) Greet(context sliced.Context) {
	fmt.Println(context.Exec("Hi"))
	fmt.Println("你好宇宙")
}

var Greeter greeting
