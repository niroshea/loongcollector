package rename

import (
	"hash/fnv"
	"runtime"
	"sync"
	"sync/atomic"
)

type AppStatShard struct {
	mu    sync.RWMutex
	stats map[string]*uint64
}

type AppSizeAggregator struct {
	shards []AppStatShard
}

func NewAppSizeAggregator() *AppSizeAggregator {
	a := &AppSizeAggregator{
		shards: make([]AppStatShard, shardCount),
	}
	for i := range shardCount {
		a.shards[i].stats = make(map[string]*uint64)
	}
	return a
}

func (a *AppSizeAggregator) getShard(key string) *AppStatShard {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &a.shards[h.Sum32()&(shardCount-1)]
}

func (a *AppSizeAggregator) Add(key string, size int) {
	shard := a.getShard(key)
	shard.mu.RLock()
	counter, ok := shard.stats[key]
	shard.mu.RUnlock()
	if !ok {
		// 如果没有，就写入一个新的
		shard.mu.Lock()
		if counter, ok = shard.stats[key]; !ok {
			shard.stats[key] = new(uint64)
			counter = shard.stats[key]
		}
		shard.mu.Unlock()
	}
	// 原子加
	atomic.AddUint64(counter, uint64(size))
}

func (a *AppSizeAggregator) Delete(key string) {
	shard := a.getShard(key)
	//
	shard.mu.Lock()
	delete(shard.stats, key)
	shard.mu.Unlock()
}

func (a *AppSizeAggregator) Get(key string) uint64 {
	shard := a.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if val, ok := shard.stats[key]; ok {
		return atomic.LoadUint64(val)
	}
	return 0
}

func (a *AppSizeAggregator) Snapshot() map[string]uint64 {
	result := make(map[string]uint64)
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

func nextPowerOfTwo(n uint32) uint32 {
	if n <= 1 {
		return 1
	}
	n--

	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16

	n++
	return n
}

// 分片数量（应为2的幂）
const DefaultShardCount uint32 = 1 << 5

func getShardCount() uint32 {
	count := nextPowerOfTwo(uint32(runtime.NumCPU() * 4))
	if count <= DefaultShardCount {
		return DefaultShardCount
	}
	return count
}

var shardCount = getShardCount()
