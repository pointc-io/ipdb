package raftfastlog

import "os"

type FileStore struct {

}

type AOFLog struct {
	indexFile *os.File
	logFile *os.File
	FirstIndex uint64
}
