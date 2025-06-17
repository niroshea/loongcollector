package elasticsearch

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	targetContentPrefix = "content."
	targetTagPrefix     = "tag."
	tagHostIP           = "host.ip"
	//
	contentContainerKey        = targetContentPrefix + "container"
	contentNamespaceKey        = targetContentPrefix + "namespace"
	contentPodNameKey          = targetContentPrefix + "pod"
	tagNodeIPKey               = targetTagPrefix + tagHostIP
	spaceStr            string = " "
)

// 定义一个全局 CounterVec
var bytesWritten = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "app_log_bytes_written_total",
		Help: "Total number of App log bytes written",
	},
	[]string{"lc_container", "lc_namespace", "lc_pod", "lc_node"},
)

func init() {
	// 创建一个新的注册器，不注册默认的 Go 和进程指标
	reg := prometheus.NewRegistry()
	// 注册指标
	reg.MustRegister(bytesWritten)
	// 采集数据
	go flushToPrometheus()
	// 暴露 /metrics HTTP 接口
	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		if err := http.ListenAndServe(":8080", nil); err != nil {
			panic(err)
		}
	}()
}

func xKey(valueMap map[string]string) string {
	return valueMap[contentContainerKey] + spaceStr + valueMap[contentNamespaceKey] + spaceStr + valueMap[contentPodNameKey] + spaceStr + valueMap[tagNodeIPKey]
}
func xKeys(key string) (container, namespace, pod, node string, ok bool) {
	vlist := strings.Fields(key)
	if len(vlist) != 4 {
		return "", "", "", "", false
	}
	return vlist[0], vlist[1], vlist[2], vlist[3], true
}

var safeCountMap = NewSafeMap()

// 注册一个标签组合（并初始化局部计数器）
func appDataLenAdd(valueMap map[string]string, dataLen uint64) {
	key := xKey(valueMap)
	cPtr := safeCountMap.Get(key)
	if cPtr == nil {
		cPtr = safeCountMap.Regist(key)
	}
	atomic.AddUint64(cPtr, dataLen)
}

func flushToPrometheus() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		for _, key := range safeCountMap.Keys() {
			container, namespace, pod, node, ok := xKeys(key)
			if !ok {
				continue
			}
			delta := atomic.LoadUint64(safeCountMap.Get(key))
			if delta > 0 {
				bytesWritten.WithLabelValues(container, namespace, pod, node).Add(float64(delta))
			}
		}
	}
}

type SafeMap struct {
	mu sync.RWMutex
	m  map[string]*uint64
}

func NewSafeMap() *SafeMap {
	return &SafeMap{m: make(map[string]*uint64)}
}

func (sm *SafeMap) Get(key string) *uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.m[key]
}

func (sm *SafeMap) Regist(key string) *uint64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.m[key] == nil {
		sm.m[key] = new(uint64)
	}
	return sm.m[key]
}

func (sm *SafeMap) Keys() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	keys := make([]string, 0, len(sm.m))
	for k := range sm.m {
		keys = append(keys, k)
	}
	return keys
}
