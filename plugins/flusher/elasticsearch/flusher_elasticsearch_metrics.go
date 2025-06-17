package elasticsearch

import (
	"net/http"
	"strings"
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

var (
	ncMap = NewConcurrentMap()
)

func appDataLenAdd(valueMap map[string]string, dataLen uint64) {
	key := xKey(valueMap)
	ptr, ok := ncMap.Get(key)
	if !ok {
		ptr = ncMap.Regist(key)
	}
	atomic.AddUint64(ptr, dataLen)
}

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

func flushToPrometheus() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		ncMap.Range(func(key string, val *uint64) {
			container, namespace, pod, node, ok := xKeys(key)
			if !ok || val == nil {
				return
			}
			delta := atomic.LoadUint64(val)
			if delta > 0 {
				bytesWritten.WithLabelValues(container, namespace, pod, node).Add(float64(delta))
			}
		})
	}
}
