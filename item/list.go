package item

const (
	MaxMapPackValueLen = 32
)

// <len><path-0><encoded-val><path-1><encoded-val><path-2><encoded-val>
// 128-name.last5Smithname.first5George
type MapPack []byte

func (m *MapPack) PutString(name string, value string) {
	if len(*m) == 0 {
		*m = NewMapPack()
	}
}

func NewMapPack() MapPack {
	return MapPack(make([]byte, 0, 6))
}

//
func getHeaderValue(buf []byte) (offset int, length int) {
	l := len(buf)
	if l == 0 {
		return 0, 0
	}

	return 0, 0
}

func putHeaderValue() {

}
