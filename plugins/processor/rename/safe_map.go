package rename

import (
	"runtime"
	"sync"
	"sync/atomic"

	xxhash "github.com/cespare/xxhash/v2"
)

type AppStatShard struct {
	mu    sync.RWMutex
	stats map[string][]*uint64
}

type AppSizeAggregator struct {
	shards    []AppStatShard
	shardsLen uint32
}

func NewAppSizeAggregator(initShardCount ...uint32) *AppSizeAggregator {
	a := &AppSizeAggregator{
		shardsLen: defaultShardLen,
	}
	if len(initShardCount) > 0 {
		if isPowerOf2(initShardCount[0]) {
			a.shardsLen = initShardCount[0]
		} else {
			panic("input shard count need > 2 and need 2^N")
		}
	}
	a.shards = make([]AppStatShard, a.shardsLen)
	for i := range a.shards {
		a.shards[i].stats = make(map[string][]*uint64)
	}
	return a
}

func (a *AppSizeAggregator) getShard(key string) *AppStatShard {
	h := uint32(xxhash.Sum64String(key))
	return &a.shards[h&(a.shardsLen-1)]
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
	result := make(map[string][]uint64)
	for i := range a.shards {
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
	perf := make([]int, a.shardsLen)
	for i := range a.shards {
		shard := &a.shards[i]
		shard.mu.RLock()
		perf[i] = len(shard.stats)
		shard.mu.RUnlock()
	}
	return perf
}

// 分片数量（应为2的幂）
func nextPower2(n uint32) uint32 {
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

func isPowerOf2(n uint32) bool {
	return n > 2 && ((n & (n - 1)) == 0)
}

// 分片数量（应为2的幂）,最小为4
var defaultShardLen = max(nextPower2(uint32(runtime.NumCPU()))*4, 4)
