package main

import (
	"fmt"

	"github.com/pointc-io/sliced"
)

type greeting string

func (g greeting) Greet(context sliced.Context) {
	sliced.SetContextName("Changed")
	fmt.Println(context.Exec("Hi"))
	fmt.Println("Hello Universe")
}

// exported
var Greeter greeting
