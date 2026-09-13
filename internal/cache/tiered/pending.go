package tiered

import "sync"

type pendingWriteState struct {
	count   int
	lastErr error
}

type pendingWrites struct {
	mu     sync.Mutex
	cond   *sync.Cond
	states map[string]*pendingWriteState
}

func newPendingWrites() *pendingWrites {
	pending := &pendingWrites{states: make(map[string]*pendingWriteState)}
	pending.cond = sync.NewCond(&pending.mu)
	return pending
}

func (p *pendingWrites) add(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.states[key]
	if state == nil {
		state = &pendingWriteState{}
		p.states[key] = state
	}
	state.count++
}

func (p *pendingWrites) cancel(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.states[key]
	if state == nil {
		return
	}
	if state.count > 0 {
		state.count--
	}
	if state.count == 0 && state.lastErr == nil {
		delete(p.states, key)
	}
	p.cond.Broadcast()
}

func (p *pendingWrites) complete(key string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.states[key]
	if state == nil {
		return
	}
	if state.count > 0 {
		state.count--
	}
	state.lastErr = err
	if state.count == 0 && err == nil {
		delete(p.states, key)
	}
	p.cond.Broadcast()
}

func (p *pendingWrites) wait(key string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		state := p.states[key]
		if state == nil {
			return nil
		}
		if state.count == 0 {
			return state.lastErr
		}
		p.cond.Wait()
	}
}

func (p *pendingWrites) status(key string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.states[key]
	if state == nil {
		return false, nil
	}
	return state.count > 0, state.lastErr
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
