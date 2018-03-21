package api

import (
	"github.com/hashicorp/raft"
	"github.com/pointc-io/sliced/command"
	"github.com/pointc-io/sliced/item"
	"github.com/pointc-io/sliced/service"
)

type Environment interface {
	Master() Master
}

type Master interface {
	service.Service

	Master() Slice

	SliceOf(key string) Slice

	SliceFor(id int) Slice
}

type Slice interface {
	service.Service

	ID() int

	Leader() (string, error)

	Stats() map[string]string

	State() raft.RaftState

	ReadSortedSet(key string, fn func(set *item.SortedSet)) error

	WriteSortedSet(key string, fn func(set *item.SortedSet)) error

	Commit(ctx *command.Context, changes *command.ChangeSet)
}

type Module interface {
}
