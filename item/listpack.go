package item

import "math"

const (
	LP_HDR_SIZE             = 6
	LP_HDR_NUMELE_UNKNOWN   = math.MaxUint16
	LP_MAX_INT_ENCODING_LEN = 9
	LP_MAX_BACKLEN_SIZE     = 5
	LP_MAX_ENTRY_BACKLEN    = uint64(34359738367)
	LP_ENCODING_INT         = 0
	LP_ENCODING_STRING      = 1

	LP_ENCODING_7BIT_UINT      = 0
	LP_ENCODING_7BIT_UINT_MASK = 0x80

	LP_ENCODING_6BIT_STR      = 0x80
	LP_ENCODING_6BIT_STR_MASK = 0xC0

	LP_ENCODING_13BIT_INT      = 0xC0
	LP_ENCODING_13BIT_INT_MASK = 0xE0

	LP_ENCODING_12BIT_STR      = 0xE0
	LP_ENCODING_12BIT_STR_MASK = 0xF0

	LP_ENCODING_16BIT_INT      = 0xF1
	LP_ENCODING_16BIT_INT_MASK = 0xFF

	LP_ENCODING_24BIT_INT      = 0xF2
	LP_ENCODING_24BIT_INT_MASK = 0xFF

	LP_ENCODING_32BIT_INT      = 0xF3
	LP_ENCODING_32BIT_INT_MASK = 0xFF

	LP_ENCODING_64BIT_INT      = 0xF4
	LP_ENCODING_64BIT_INT_MASK = 0xFF

	LP_ENCODING_32BIT_STR      = 0xF0
	LP_ENCODING_32BIT_STR_MASK = 0xFF

	LP_EOF = 0xFF
)

func LP_ENCODING_IS_7BIT_UINT(b byte) bool {
	return b&LP_ENCODING_7BIT_UINT_MASK == LP_ENCODING_7BIT_UINT
}

func LP_ENCODING_IS_6BIT_STR(b byte) bool {
	return b&LP_ENCODING_6BIT_STR_MASK == LP_ENCODING_6BIT_STR
}

func LP_ENCODING_IS_13BIT_UINT(b byte) bool {
	return b&LP_ENCODING_13BIT_INT_MASK == LP_ENCODING_13BIT_INT
}

func LP_ENCODING_IS_12BIT_STR(b byte) bool {
	return b&LP_ENCODING_12BIT_STR_MASK == LP_ENCODING_12BIT_STR
}

func LP_ENCODING_IS_16BIT_UINT(b byte) bool {
	return b&LP_ENCODING_16BIT_INT_MASK == LP_ENCODING_16BIT_INT
}

func LP_ENCODING_IS_24BIT_UINT(b byte) bool {
	return b&LP_ENCODING_24BIT_INT_MASK == LP_ENCODING_24BIT_INT
}

func LP_ENCODING_IS_32BIT_UINT(b byte) bool {
	return b&LP_ENCODING_32BIT_INT_MASK == LP_ENCODING_32BIT_INT
}

func LP_ENCODING_IS_64BIT_UINT(b byte) bool {
	return b&LP_ENCODING_64BIT_INT_MASK == LP_ENCODING_64BIT_INT
}

func LP_ENCODING_IS_32BIT_STR(b byte) bool {
	return b&LP_ENCODING_32BIT_STR_MASK == LP_ENCODING_32BIT_STR
}

func LP_ENCODING_6BIT_STR_LEN(p []byte) uint32 {
	return uint32(p[0] & 0x3F)
}

func LP_ENCODING_12BIT_STR_LEN(p []byte) uint32 {
	return uint32(p[0])<<8 | uint32(p[1])
}

func LP_ENCODING_32IT_STR_LEN(p []byte) uint32 {
	return uint32(p[1])<<0 | uint32(p[2])<<8 | uint32(p[3])<<16 | uint32(p[4])<<24
}

func lpGetTotalBytes(p []byte) uint32 {
	return uint32(p[0])<<0 | uint32(p[1])<<8 | uint32(p[2])<<16 | uint32(p[3])<<24
}

func lpGetNumElements(p []byte) uint32 {
	return uint32(p[4])<<0 | uint32(p[5])<<8
}

func lpSetTotalBytes(p []byte, v uint32) {
	p[0] = byte(v & 0xff)
	p[1] = byte(v >> 8 & 0xff)
	p[2] = byte(v >> 16 & 0xff)
	p[3] = byte(v >> 24 & 0xff)
}

func lpSetNumElements(p []byte, v uint32) {
	p[4] = byte(v & 0xff)
	p[5] = byte(v >> 8 & 0xff)
}

func lpStringToInt64(p []byte, value *int64) bool {
	slen := len(p)

	if slen == 0 {
		return false
	}

	if slen == 1 && p[0] == '0' {
		*value = 0
		return true
	}

	var plen = 0
	var negative = 0
	var v uint64 = 0

	pi := 0

	if p[pi] == '-' {
		negative = 1
		plen++

		if plen == slen {
			return false
		}

		pi++
	}

	if p[pi] >= '1' && p[pi] <= '9' {
		v = uint64(p[0] -'0')
		pi++
		plen++
	} else if p[pi] == '0' && slen == 1 {
		*value = 0
		return true
	} else {
		return false
	}

	for plen < slen && p[pi] >= '0' && p[0] <= '9' {
		if v > math.MaxUint64 / 10 {
			return false
		}
		v *= 10

		if v > (math.MaxUint64 - uint64(p[pi]-'0')) {
			return false
		}
		v += uint64(p[pi]-'0')
		pi++
		plen++
	}

	if plen < slen {
		return false
	}

	if negative == 1 {
		if v > uint64(-(math.MinInt64 + 1) + 1) {
			return false
		}
		*value = -int64(v)
	} else {
		if v > math.MaxInt64 {
			return false
		}
		*value = int64(v)
	}
	return true
}

//func lpEncodeGetType(buf []byte, size int, intenc []byte, enclen int) {
//	var v int64
//	if lpStringToInt64(buf, size, &v) {
//
//	}
//}

func lpEncodeString(buf []byte, offset int, s []byte) {
	l := len(s)

	if len(s) < 64 {
		buf[offset] = byte(len(s) | LP_ENCODING_6BIT_STR)
	} else if len(s) < 4096 {
		buf[0] = byte((l >> 8) | LP_ENCODING_12BIT_STR)
		buf[offset + 1] = byte(l & 0xff)
		for i := 0; i < l; i++ {
			buf[offset + 2 + i] = s[i]
		}
	} else {
		buf[offset] = byte((l >> 8) | LP_ENCODING_32BIT_STR)
		buf[offset + 1] = byte(l & 0xff)
		buf[offset + 2] = byte((l >> 8) & 0xff)
		buf[offset + 3] = byte((l >> 16) & 0xff)
		buf[offset + 4] = byte((l >> 24) & 0xff)
		for i := 0; i < l; i++ {
			buf[offset + 5 + i] = s[i]
		}
	}
}