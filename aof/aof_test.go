package aof

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"

	"github.com/edsrzf/mmap-go"
)

var testPath = "./aof/log.txt"

var aof *AOF
var err error

func init() {
	aof, err = Open("./aof/log.txt", os.Getpagesize()*os.Getpagesize()*10, VarintLengthOpener)
}

func openFile(flags int) *os.File {
	f, err := os.OpenFile(testPath, flags, 0755)
	if err != nil {
		panic(err.Error())
	}
	return f
}

func TestOpen(t *testing.T) {
	for i := 0; i < 1000000; i++ {
		aof.AppendWithLength([]byte("hite"))
	}
	aof.flush()
}

func BenchmarkAOF_Append(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	buf := make([]byte, 8)
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(buf[0:], uint64(i))
		aof.AppendWithLength(buf)
	}
	aof.flush()
}

func BenchmarkAOF_AppendFile(b *testing.B) {
	file, _ := os.OpenFile("./aof/loga.txt", os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.O_CREATE, 0755)

	for i := 0; i < b.N; i++ {
		file.Write([]byte("h"))
	}
}

func TestMmap(t *testing.T) {
	f := openFile(os.O_RDWR | os.O_CREATE | os.O_TRUNC)
	//f.Write([]byte(""))

	//var length = 1024
	f.Truncate(int64(os.Getpagesize() * 4))

	//syscall.Ftruncate(int(f.Fd()), int64(1024))

	//m, err := syscall.Mmap(
	//	int(f.Fd()),
	//	0,
	//	length,
	//	//syscall.PROT_READ|syscall.PROT_WRITE,
	//	syscall.PROT_WRITE|syscall.PROT_READ,
	//	syscall.MAP_SHARED,
	//)
	//ptr := (*reflect.SliceHeader)(unsafe.Pointer(&m))

	//fmt.Println(ptr)

	m, err := mmap.MapRegion(f, 16, mmap.RDWR, 0, 0)

	//m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}

	copy(m, []byte("testing.name"))

	fmt.Printf("%s\n", m[:])

	//_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, ptr.Data, uintptr(3), syscall.MS_SYNC)
	//_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&m)), uintptr(length), syscall.MS_SYNC)

	//fmt.Println(errno)

	m.Flush()
	//f.Sync()
	//f.Close()

	//syscall.SYS_MSYNC

	//aof, err := Open("./aof/log.db", regionSizeMin)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//aof.Append([]byte("hi"))
	//err = aof.Flush()
	//if err != nil {
	//	t.Fatal(err)
	//}
	//err = aof.Close()
	//if err != nil {
	//	t.Fatal(err)
	//}
}
