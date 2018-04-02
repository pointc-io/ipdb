package aof

type PointerFile struct {
	aof   *AOF
	min   uint64
	max   uint64
}

// Points to a mapping in the table
type pointerRegion struct {
}

type pointerEntry struct {
	fid    uint64
	offset uint64
}
