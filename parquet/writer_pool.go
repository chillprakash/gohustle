package parquet

import (
	"context"
	"sync"

	"gohustle/logger"
	"gohustle/proto"
)

const (
	defaultNumWorkers = 10
	defaultBufferSize = 1000
	defaultQueueSize  = 10000
	ticksDir          = "data/ticks"
)

type WriteRequest struct {
	TargetFile string
	Tick       *proto.TickData
	ResultChan chan error
}

type WriterWorker struct {
	id         int
	workChan   chan *WriteRequest
	buffer     map[string][]*proto.TickData
	bufferSize int
	ctx        context.Context
	log        *logger.Logger
	baseDir    string
}

type WriterPool struct {
	workers    []*WriterWorker
	workChan   chan *WriteRequest
	numWorkers int
	bufferSize int
	ctx        context.Context
	cancel     context.CancelFunc
	log        *logger.Logger
	wg         sync.WaitGroup
}

// ... rest of the writer pool code ...
