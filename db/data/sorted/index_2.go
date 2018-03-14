package sorted

type IndexIterator func(key IndexItem) bool

//
type indexFactory func(idx *SetIndex, item *Item) IndexItem

//
type projector func(item *Item) (string, bool)

//
type intProjector func(item *Item) (int64, bool)

//
type floatProjector func(item *Item) (float64, bool)

//
type spatialProjector func(item *Item) ([]float64, []float64, bool)


