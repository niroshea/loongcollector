package elasticsearch

import (
	"sync"

	xhash "github.com/cespare/xxhash/v2"
)

// 分片数量（必须是2的幂次）
const (
	shardsLen = 1 << 10
	shardMask = shardsLen - 1
)

type ConcurrentMap struct {
	shards [shardsLen]*shard
}

type shard struct {
	//sync.RWMutex
	data sync.Map // map[string]*uint64
}

func NewConcurrentMap() *ConcurrentMap {
	cm := &ConcurrentMap{}
	for i := range cm.shards {
		cm.shards[i] = &shard{}
	}
	return cm
}

// 计算key对应的分片索引
func (cm *ConcurrentMap) getShardIndex(key string) uint32 {
	return uint32(xhash.Sum64String(key)) & shardMask
}

func (cm *ConcurrentMap) Get(key string) (*uint64, bool) {
	shard := cm.shards[cm.getShardIndex(key)]
	val, ok := shard.data.Load(key)
	if !ok {
		return nil, false
	}
	return val.(*uint64), true
}

func (cm *ConcurrentMap) Regist(key string) *uint64 {
	shard := cm.shards[cm.getShardIndex(key)]
	val, _ := shard.data.LoadOrStore(key, new(uint64))
	return val.(*uint64)
}

func (cm *ConcurrentMap) Range(f func(key string, val *uint64)) {
	for i := range shardsLen {
		shard := cm.shards[i]
		shard.data.Range(func(k, v any) bool {
			key := k.(string)
			val := v.(*uint64)
			f(key, val)
			return true // 继续遍历
		})
	}
}
