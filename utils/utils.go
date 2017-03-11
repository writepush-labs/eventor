package utils

import(
	"sync"
	"time"
)

type PersistedCounterMap struct {
	sync.RWMutex
	counters map[string]int64
	flushing bool
	modifications int64
}

func (pcm *PersistedCounterMap) Increment(key string) int64 {
	pcm.Lock()
	defer pcm.Unlock()
	pcm.counters[key]++
	pcm.modifications++
	return pcm.counters[key]
}

func (pcm *PersistedCounterMap) Decrement(key string) int64 {
	pcm.Lock()
	defer pcm.Unlock()
	pcm.counters[key]--
	pcm.modifications++
	return pcm.counters[key]
}

func (pcm *PersistedCounterMap) IncrementMulti(keys ...string) {
	pcm.Lock()
	defer pcm.Unlock()

	for _, key := range keys {
		pcm.counters[key]++
	}

	pcm.modifications++
}

func (pcm *PersistedCounterMap) DecrementMulti(keys ...string) {
	pcm.Lock()
	defer pcm.Unlock()

	for _, key := range keys {
		pcm.counters[key]--
	}

	pcm.modifications++
}

func (pcm *PersistedCounterMap) Get(key string) int64 {
	pcm.RLock()
	defer pcm.RUnlock()
	return pcm.counters[key]
}

func (pcm *PersistedCounterMap) Set(key string, val int64) {
	pcm.Lock()
	defer pcm.Unlock()
	pcm.counters[key] = val
	pcm.modifications++
}

func (pcm *PersistedCounterMap) Load(counters map[string]int64) {
	pcm.Lock()
	defer pcm.Unlock()
	pcm.counters = counters
}

func (pcm *PersistedCounterMap) IsFlushing() bool {
	pcm.RLock()
	defer pcm.RUnlock()

	return pcm.flushing
}

func (pcm *PersistedCounterMap) RequiresFlushing() bool {
	pcm.RLock()
	defer pcm.RUnlock()

	return pcm.modifications > 0
}

func (pcm *PersistedCounterMap) GetCopy() map[string]int64 {
	pcm.RLock()
	defer pcm.RUnlock()
	var copy map[string]int64

	for k, v := range pcm.counters {
		copy[k] = v
	}

	return copy
}

func (pcm *PersistedCounterMap) setFlushingFlag(f bool) {
	pcm.Lock()
	pcm.flushing = f

	if ! f {
		pcm.modifications = 0
	}
	pcm.Unlock()
}

func CreatePersistedCounterMap(flush func(c map[string]int64), checkpointInterval int64) *PersistedCounterMap {
	pcm := &PersistedCounterMap{
		counters: make(map[string]int64),
	}

	go func() {
		for range time.Tick(time.Duration(checkpointInterval) * time.Second) {
			if pcm.IsFlushing() || ! pcm.RequiresFlushing() {
				continue
			}

			pcm.setFlushingFlag(true)

			pcm.RLock()
			flush(pcm.counters)
			pcm.RUnlock()

			pcm.setFlushingFlag(false)
		}
	}()

	return pcm
}