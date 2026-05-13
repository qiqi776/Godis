package wal

import (
	"os"
	"sync"
	
)

const (
	defaultSegmentSize = 64 << 20 // 默认段大小为64MB
	filePrefix 		   = "WAL-"
)

type Options struct {
	SegmentSize int64
}

type Store struct {
	mu  		sync.Mutex
	dir 		string
	segmentSize int64
	fileNum 	uint64
	file    	*os.File
	offset  	int64
}

