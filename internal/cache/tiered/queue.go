package tiered

import (
	"sync"
	"time"
)

type writeQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	empty    *sync.Cond

	items []writeOperation
	head  int
	tail  int
	size  int

	pendingBytes int64
	pending      int
	maxBytes     int64
	closed       bool
}

func newWriteQueue(capacity int, maxBytes int64) *writeQueue {
	queue := &writeQueue{
		items:    make([]writeOperation, capacity),
		maxBytes: maxBytes,
	}
	queue.notEmpty = sync.NewCond(&queue.mu)
	queue.notFull = sync.NewCond(&queue.mu)
	queue.empty = sync.NewCond(&queue.mu)
	return queue
}

func (q *writeQueue) enqueue(operation writeOperation, timeout time.Duration) error {
	operationBytes := writeOperationBytes(operation)
	if operationBytes > q.maxBytes {
		return ErrWriteQueueValueTooLarge
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}

	deadline := time.Now().Add(timeout)
	for q.pending == len(q.items) || q.pendingBytes+operationBytes > q.maxBytes {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ErrWriteQueueTimeout
		}

		timer := time.AfterFunc(remaining, func() {
			q.mu.Lock()
			q.notFull.Broadcast()
			q.mu.Unlock()
		})
		q.notFull.Wait()
		timer.Stop()

		if q.closed {
			return ErrClosed
		}
	}

	q.items[q.tail] = operation
	q.tail = (q.tail + 1) % len(q.items)
	q.size++
	q.pending++
	q.pendingBytes += operationBytes
	q.notEmpty.Signal()
	return nil
}

func (q *writeQueue) dequeue() (writeOperation, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.size == 0 && !q.closed {
		q.notEmpty.Wait()
	}
	if q.size == 0 {
		return writeOperation{}, false
	}

	operation := q.items[q.head]
	q.items[q.head] = writeOperation{}
	q.head = (q.head + 1) % len(q.items)
	q.size--
	return operation, true
}

func (q *writeQueue) complete(operation writeOperation) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.pending--
	q.pendingBytes -= writeOperationBytes(operation)
	q.notFull.Broadcast()
	if q.pending == 0 {
		q.empty.Broadcast()
	}
}

func (q *writeQueue) waitEmpty() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.pending > 0 {
		q.empty.Wait()
	}
}

func (q *writeQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
	q.empty.Broadcast()
}

func writeOperationBytes(operation writeOperation) int64 {
	// The queue retains the underlying byte slice, so cap is the memory that can
	// actually stay reachable while the operation is pending.
	return int64(cap(operation.value))
}
