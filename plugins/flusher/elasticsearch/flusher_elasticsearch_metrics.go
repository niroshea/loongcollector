package elasticsearch

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

	spaceStr string = " "
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

func (m *Metrics) Register(key string) *ByteCounter {
	if val, ok := m.counters.Load(key); ok {
		return val.(*ByteCounter)
	}
	xlist := strings.Fields(key)
	if len(xlist) != 3 {
		return nil
	}
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "app_log_bytes_written_total",
		Help: "Total number of App log bytes written",
		ConstLabels: prometheus.Labels{
			"lc_container": xlist[0],
			"lc_namespace": xlist[1],
			"lc_node":      xlist[2],
		},
	})
	m.reg.MustRegister(counter)

	bc := &ByteCounter{counter: counter}
	m.counters.Store(key, bc)
	return bc
}

func init() {
	for i := range goThreadNum {
		goThreadMap[i] = make(map[string]*uint64)
		goThreadChan[i] = make(chan struct{}, 1)
	}
	handleMetricsData() // 获取logLen
	go readMapTicker()  // ticker
	go promDataHandle() // 获取map

	go func() {
		gin.SetMode(gin.ReleaseMode)
		r := gin.Default()
		// 提供 /metrics 接口
		r.GET("/metrics", gin.WrapH(promhttp.HandlerFor(pMetrics.reg, promhttp.HandlerOpts{})))
		// 启动服务
		r.Run(":8080")
	}()
}

type MetricsData struct {
	Container string
	Namespace string
	NodeIP    string
	DataLen   int
}

func (m *MetricsData) Reset() {
	m.Container = ""
	m.Namespace = ""
	m.NodeIP = ""
	m.DataLen = 0
}

func (m *MetricsData) Key() string {
	return strings.Join([]string{m.Container, m.Namespace, m.NodeIP}, spaceStr)
}

// 创建一个 sync.Pool 用于复用 MetricsData 对象
var metricsDataPool = sync.Pool{
	New: func() any {
		return &MetricsData{}
	},
}

// 获取一个 MetricsData 实例
func getMetricsData() *MetricsData {
	return metricsDataPool.Get().(*MetricsData)
}

// 归还一个 MetricsData 实例到 Pool，并重置字段
func putMetricsData(md *MetricsData) {
	// 清空字段，防止下次误用残留数据
	md.Reset()
	metricsDataPool.Put(md)
}

var metricsChan = make(chan *MetricsData, 1024)

var fullMapChan = make(chan map[string]*uint64, goThreadNum)

func sendMetricsData(valueMap map[string]string, dLen int) {
	tmp := getMetricsData()
	tmp.Container = valueMap[contentContainerKey]
	tmp.Namespace = valueMap[contentNamespaceKey]
	tmp.NodeIP = valueMap[tagNodeIPKey]
	tmp.DataLen = dLen
	//
	metricsChan <- tmp
}

const goThreadNum = 10

var goThreadMap = new([goThreadNum]map[string]*uint64)
var goThreadChan = new([goThreadNum]chan struct{})

// 处理
func handleMetricsData() {
	for i := range goThreadNum {
		go func(idx int) {
			goThMap := goThreadMap[i]
			for {
				select {
				case msg := <-metricsChan:
					mkey := msg.Key()
					ptr := goThMap[mkey]
					if ptr == nil {
						ptr = new(uint64)
						goThMap[mkey] = ptr
					}
					atomic.AddUint64(ptr, uint64(msg.DataLen))
					putMetricsData(msg) // 归还MetricsData对象
				case <-goThreadChan[i]:
					xmap := make(map[string]*uint64, len(goThMap))
					for k, v := range goThMap {
						xmap[k] = v
					}
					fullMapChan <- xmap
				}
			}
		}(i)
	}
}

// 接收map数据
func promDataHandle() {
	for xmap := range fullMapChan {
		for key, cnt := range xmap {
			cntVal := atomic.LoadUint64(cnt)
			if cntVal < 1 {
				continue
			}
			x := pMetrics.Register(key)
			if x != nil {
				x.counter.Add(float64(cntVal)) // 写入数据计数
			}
		}
	}
}

// ticker
func readMapTicker() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for i := range goThreadNum {
			goThreadChan[i] <- struct{}{}
		}
	}
}
