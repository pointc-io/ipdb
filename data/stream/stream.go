package stream

import "github.com/armon/go-radix"

type Stream struct {
	radx radix.Tree
}