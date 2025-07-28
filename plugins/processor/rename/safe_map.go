package rename

import (
	"sync"
	"sync/atomic"

	xxhash "github.com/cespare/xxhash/v2"
)

// 素数求模，散列程度更好 11, 13, 17, 19,23, 29,31, 37,41, 43, 47,53, 59,61, 67,71, 73, 79
// const 常数求模，编译器优化后，性能与位运算差不多
// 分片并不是越大越好，CPU核数*4左右在通常情况下较优
const shardsLen = 31

type AppStatShard struct {
	mu    sync.RWMutex
	stats map[string][]*uint64
}

type AppSizeAggregator struct {
	shards []AppStatShard
}

func NewAppSizeAggregator() *AppSizeAggregator {
	a := &AppSizeAggregator{
		shards: make([]AppStatShard, shardsLen),
	}
	for i := range shardsLen {
		a.shards[i].stats = make(map[string][]*uint64)
	}
	return a
}

func (a *AppSizeAggregator) getShard(key string) *AppStatShard {
	h64 := xxhash.Sum64String(key)
	return &a.shards[h64%shardsLen]
}

func (a *AppSizeAggregator) Add(key string, bytesLen, logCount int) {
	shard := a.getShard(key)
	shard.mu.RLock()
	counter, ok := shard.stats[key]
	shard.mu.RUnlock()
	if !ok {
		// 如果没有，就写入一个新的
		shard.mu.Lock()
		if counter, ok = shard.stats[key]; !ok {
			shard.stats[key] = []*uint64{new(uint64), new(uint64)}
			counter = shard.stats[key]
		}
		shard.mu.Unlock()
	}
	// 原子加
	atomic.AddUint64(counter[0], uint64(bytesLen))
	atomic.AddUint64(counter[1], uint64(logCount))
}

func (a *AppSizeAggregator) Delete(key string) {
	shard := a.getShard(key)
	//
	shard.mu.Lock()
	delete(shard.stats, key)
	shard.mu.Unlock()
}

func (a *AppSizeAggregator) Get(key string) (uint64, uint64) {
	shard := a.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if val, ok := shard.stats[key]; ok {
		return atomic.LoadUint64(val[0]), atomic.LoadUint64(val[1])
	}
	return 0, 0
}

func (a *AppSizeAggregator) Snapshot() map[string][]uint64 {
	result := make(map[string][]uint64, shardsLen)
	for i := range shardsLen {
		shard := &a.shards[i]
		shard.mu.RLock()
		for k, v := range shard.stats {
			result[k] = []uint64{atomic.LoadUint64(v[0]), atomic.LoadUint64(v[1])}
		}
		shard.mu.RUnlock()
	}
	return result
}

func (a *AppSizeAggregator) HashDistribution() []int {
	perf := make([]int, shardsLen)
	for i := range a.shards {
		shard := &a.shards[i]
		shard.mu.RLock()
		perf[i] = len(shard.stats)
		shard.mu.RUnlock()
	}
	return perf
}
