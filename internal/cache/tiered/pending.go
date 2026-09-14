package tiered

import "sync"

// pendingWrites tracks asynchronous L2 writes per key. Get waits on this state
// before falling back to L2 so it cannot observe an older value while a local
// write-behind operation is still pending.
type pendingWrites struct {
	mu     sync.Mutex
	cond   *sync.Cond
	states map[string]int
}

func newPendingWrites() *pendingWrites {
	pending := &pendingWrites{states: make(map[string]int)}
	pending.cond = sync.NewCond(&pending.mu)
	return pending
}

func (p *pendingWrites) add(key string) {
	p.mu.Lock()
	p.states[key]++
	p.mu.Unlock()
}

func (p *pendingWrites) cancel(key string) {
	p.finish(key)
}

func (p *pendingWrites) complete(key string) {
	p.finish(key)
}

func (p *pendingWrites) finish(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	count := p.states[key]
	if count <= 1 {
		delete(p.states, key)
	} else {
		p.states[key] = count - 1
	}
	p.cond.Broadcast()
}

func (p *pendingWrites) wait(key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.states[key] > 0 {
		p.cond.Wait()
	}
	return nil
}

func (p *pendingWrites) status(key string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.states[key] > 0, nil
}

func (p *pendingWrites) clear(key string) {
	p.mu.Lock()
	delete(p.states, key)
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *pendingWrites) clearAll() {
	p.mu.Lock()
	clear(p.states)
	p.cond.Broadcast()
	p.mu.Unlock()
}
