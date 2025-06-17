package elasticsearch

import (
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	targetContentPrefix = "content."
	targetTagPrefix     = "tag."
	tagHostIP           = "host.ip"
	//
	contentContainerKey = targetContentPrefix + "container"
	contentNamespaceKey = targetContentPrefix + "namespace"
	tagNodeIPKey        = targetTagPrefix + tagHostIP
	//contentPodNameKey          = targetContentPrefix + "pod"

	spaceStr string = "_"
)

var pMetrics = &Metrics{
	reg: prometheus.NewRegistry(),
}

type ByteCounter struct {
	counter prometheus.Counter
}

type Metrics struct {
	reg      *prometheus.Registry
	counters sync.Map // map[string]*ByteCounter
}

func (m *Metrics) Register(valueMap map[string]string) *ByteCounter {
	key := valueMap[contentContainerKey] + spaceStr + valueMap[contentNamespaceKey] + spaceStr + valueMap[tagNodeIPKey]
	if val, ok := m.counters.Load(key); ok {
		return val.(*ByteCounter)
	}
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "app_log_bytes_written_total",
		Help: "Total number of App log bytes written",
		ConstLabels: prometheus.Labels{
			"lc_container": valueMap[contentContainerKey],
			"lc_namespace": valueMap[contentNamespaceKey],
			"lc_node":      valueMap[tagNodeIPKey],
		},
	})
	m.reg.MustRegister(counter)

	bc := &ByteCounter{counter: counter}
	m.counters.Store(key, bc)
	return bc
}

func init() {
	go func() {
		gin.SetMode(gin.ReleaseMode)
		r := gin.Default()
		// 提供 /metrics 接口
		r.GET("/metrics", gin.WrapH(promhttp.HandlerFor(pMetrics.reg, promhttp.HandlerOpts{})))
		// 启动服务
		r.Run(":8080")
	}()
}
