package elasticsearch

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// 分片数量（应为2的幂）
const shardCount = 1 << 5

type AppStatShard struct {
	mu    sync.RWMutex
	stats map[string]*uint64
}

type AppSizeAggregator struct {
	shards [shardCount]AppStatShard
	allLen uint64
}

func NewAppSizeAggregator() *AppSizeAggregator {
	a := &AppSizeAggregator{}
	for i := range shardCount {
		a.shards[i].stats = make(map[string]*uint64)
	}
	return a
}

func (a *AppSizeAggregator) getShard(app string) *AppStatShard {
	h := fnv.New32a()
	h.Write([]byte(app))
	return &a.shards[h.Sum32()%shardCount]
}

func (a *AppSizeAggregator) Add(app string, size int) {
	shard := a.getShard(app)
	shard.mu.RLock()
	counter, ok := shard.stats[app]
	shard.mu.RUnlock()
	if !ok {
		// 如果没有，就写入一个新的
		shard.mu.Lock()
		if counter, ok = shard.stats[app]; !ok {
			var zero uint64
			shard.stats[app] = &zero
			counter = &zero
			atomic.AddUint64(&a.allLen, 1)
		}
		shard.mu.Unlock()
	}
	// 原子加
	atomic.AddUint64(counter, uint64(size))
}

func (a *AppSizeAggregator) Get(app string) uint64 {
	shard := a.getShard(app)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if val, ok := shard.stats[app]; ok {
		return atomic.LoadUint64(val)
	}
	return 0
}

func (a *AppSizeAggregator) Len() uint64 {
	return atomic.LoadUint64(&a.allLen)
}

func (a *AppSizeAggregator) Snapshot() map[string]uint64 {
	result := make(map[string]uint64, a.Len())
	for i := range shardCount {
		shard := &a.shards[i]
		shard.mu.RLock()
		for k, v := range shard.stats {
			result[k] = atomic.LoadUint64(v)
		}
		shard.mu.RUnlock()
	}
	return result
}
