package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alibaba/ilogtail/pkg/fmtstr"
	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	ants "github.com/panjf2000/ants/v2"
	"gopkg.in/yaml.v3"
)

const (
	targetContentPrefix = "content."
	targetTagPrefix     = "tag."
	//tagHostIP           = "host.ip"
	//
	contentContainerKey = targetContentPrefix + "container"
	contentNamespaceKey = targetContentPrefix + "namespace"
	//tagNodeIPKey        = targetTagPrefix + tagHostIP
	//
	spaceStr string = " "
)

var (
	totalDuration  int64  // sendBulk函数消耗时间，累计，单位纳秒 -- 计算处理耗时
	callCount      int64  // 调用 sendBulk 次数 -- 计算处理耗时
	allBufCount    uint64 // buf 写入channel 计数 -- 计算写入速率为 W（条/秒）
	dropBatchCount uint64 // 队列满了，无法及时写入的批次
	//
	goPool *ants.Pool
	//
	goThreadNum = getGoThreadNum()
	aggMap      = NewAppSizeAggregator()
)

func getGoThreadNum() int {
	ret := bulkconf.EsBlukConfig.GoThreadNum
	if ret < 2 {
		ret = 32
	}
	log.Println("--- INFO es bulk goroutine number:", ret)
	return ret
}

func init() {
	var err error
	goPool, err = ants.NewPool(goThreadNum)
	if err != nil {
		log.Fatal(err)
	}

	go performanceLog()
}

func performanceLog() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	var old_totalDuration, old_callCount int64
	var old_allBufCount, old_dropBatchCount uint64

	var old_snapshot = make(map[string]uint64)

	for range ticker.C {
		new_totalDuration, new_callCount, new_allBufCount, new_dropBatchCount :=
			atomic.LoadInt64(&totalDuration), atomic.LoadInt64(&callCount),
			atomic.LoadUint64(&allBufCount), atomic.LoadUint64(&dropBatchCount)

		dura_60 := new_totalDuration - old_totalDuration
		count_60 := new_callCount - old_callCount

		allBufCount_60 := new_allBufCount - old_allBufCount
		dropBatch_60 := new_dropBatchCount - old_dropBatchCount

		// 每次请求耗时
		var sendBulkDura int64
		if count_60 < 1 {
			sendBulkDura = 0
		} else {
			sendBulkDura = dura_60 / count_60
		}
		log.Printf("--- INFO buf chan write [ %d ] - write rate: [ %.3f/s ] - bulk write dura [ %s ] "+
			"- drop batch current/total [ %d/%d  ] - chan read rate [ %.3f/s ] - bufChan[ %d/%d ] - min thread [ %.3f ].\n",
			new_allBufCount,
			float64(allBufCount_60)/60,
			time.Duration(sendBulkDura).String(),
			dropBatch_60,
			new_dropBatchCount,
			float64(count_60)/60,
			len(bufChan),
			cap(bufChan),
			float64(allBufCount_60)*float64(sendBulkDura)/60/1e9,
		)

		log.Printf("--- INFO ants go pool Running/Free/Cap [ %d/%d/%d ].\n", goPool.Running(), goPool.Free(), goPool.Cap())

		new_snapshot := aggMap.Snapshot()
		for appKey, tBytesN := range new_snapshot {
			log.Printf("--- INFO --- [ %s ] total bytes [ %d ],rate [ %.3f MB/s ].\n",
				appKey, tBytesN, float64(tBytesN-old_snapshot[appKey])/60/1024/1024)
		}

		old_totalDuration, old_callCount, old_allBufCount, old_dropBatchCount =
			new_totalDuration, new_callCount, new_allBufCount, new_dropBatchCount
		old_snapshot = new_snapshot
	}
}

var bulkconf = getConfig()

type BlukConfig struct {
	EsBlukConfig GoroutineConf `yaml:"es_bulk_config"`
}
type GoroutineConf struct {
	GoThreadNum     int `yaml:"goroutine"`
	BatchSizeMB     int `yaml:"batch_size_mb"`
	BulkBufChanSize int `yaml:"buffer_chan_size"`
}

func getBufChanSize() (ret int) {
	ret = bulkconf.EsBlukConfig.BulkBufChanSize
	if ret < 3 {
		ret = 100
	}
	return
}

// 读取配置文件信息
func getConfig() *BlukConfig {
	var config BlukConfig
	data, err := os.ReadFile("/usr/local/loongcollector/conf/continuous_pipeline_config/local/processor_rename.yaml")
	if err != nil {
		log.Println(err)
		return nil
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &config
}

var bufferPool = sync.Pool{
	New: func() any {
		// 预先分配一定容量，避免每次扩容
		return bytes.NewBuffer(make([]byte, 0, 2*MB))
	},
}

func getBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putBuffer(buf *bytes.Buffer) {
	// 超过最大大小的 buffer 不放回池中，防止长期占用过多内存
	if buf.Cap() > maxBatchBytes+MB {
		log.Println("--- WARN  buffer 销毁")
		return
	}
	bufferPool.Put(buf)
}

var bufChan = make(chan *bytes.Buffer, getBufChanSize())

func (f *FlusherElasticSearch) handleBufChan() {
	for v := range bufChan {
		tbuf := v
		goPool.Submit(func() {
			if err := f.sendBulk(tbuf); err != nil {
				logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "Bulk send failed: %s", err)
			}
			putBuffer(tbuf)
		})
	}
}

const (
	MB int = 1024 * 1024 // 1MB
)

var maxBatchBytes = getMaxBatchSize()

func getMaxBatchSize() int {
	bsize := bulkconf.EsBlukConfig.BatchSizeMB
	if bsize < 3 {
		bsize = 10
	}
	log.Println("--- INFO es bulk size(MB):", bsize)
	return bsize * MB
}

// var compressed bytes.Buffer
// var gw = gzip.NewWriter(&compressed)

func (f *FlusherElasticSearch) sendBulk(bulkBuf *bytes.Buffer) error {
	start := time.Now()
	defer func() {
		atomic.AddInt64(&totalDuration, time.Since(start).Nanoseconds())
		atomic.AddInt64(&callCount, 1)
	}()
	// if _, err := io.Copy(gw, bulkBuf); err != nil {
	// 	return err
	// }
	// gw.Close()

	req := esapi.BulkRequest{
		//Body: bytes.NewReader(compressed.Bytes()),
		Body: bytes.NewReader(bulkBuf.Bytes()),
		Header: http.Header{
			"Content-Encoding": {"gzip"},
			"Content-Type":     {"application/json"},
		},
	}
	res, err := req.Do(context.Background(), f.esClient)
	if err != nil {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch request fail, error", err)
		return err
	}
	defer res.Body.Close()
	//清空 buffer 并复用 gzip.Writer
	// compressed.Reset()
	// gw.Reset(&compressed) // 复用同一个 gzip.Writer 实例

	if res.StatusCode >= 400 && res.StatusCode <= 499 {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch request client error", res)
		return fmt.Errorf("err status returned: %v", res.Status())
	} else if res.StatusCode >= 500 && res.StatusCode <= 599 {
		logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch request server error", res)
		return fmt.Errorf("err status returned: %v", res.Status())
	}
	return nil
}

func (f *FlusherElasticSearch) Flush(projectName string, logstoreName string, configName string, logGroupList []*protocol.LogGroup) error {
	bulkAction := "create"
	if f.Action != "" {
		bulkAction = f.Action
	}
	f.indexKeys = append(f.indexKeys, contentContainerKey, contentNamespaceKey) //, tagNodeIPKey
	nowTime := time.Now().Local()
	for _, logGroup := range logGroupList {
		logger.Debug(f.context.GetRuntimeContext(), "[LogGroup] topic", logGroup.Topic, "logstore", logGroup.Category, "logcount", len(logGroup.Logs), "tags", logGroup.LogTags)
		serializedLogs, values, err := f.converter.ToByteStreamWithSelectedFields(logGroup, f.indexKeys)
		if err != nil {
			logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch convert log fail, error", err)
			return err
		}
		bulkBuf := getBuffer()
		var batchBytes int
		for index, logData := range serializedLogs.([][]byte) {
			esIndex := &f.Index
			valueMap := values[index]
			if f.isDynamicIndex {
				esIndex, err = fmtstr.FormatIndex(valueMap, f.Index, uint32(nowTime.Unix()))
				if err != nil {
					logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "ERROR flush elasticsearch format index fail, error", err)
					return err
				}
			}
			meta := []byte(`{"` + bulkAction + `": {"_index": "` + *esIndex + `"}}` + "\n")
			logData = append(logData, "\n"...)
			logLen := len(meta) + len(logData)
			//+spaceStr+valueMap[tagNodeIPKey]
			aggMap.Add(valueMap[contentContainerKey]+spaceStr+valueMap[contentNamespaceKey], logLen)
			//
			bulkBuf.Write(meta)
			bulkBuf.Write(logData)
			//
			batchBytes += logLen
			if batchBytes >= maxBatchBytes {
				select {
				case bufChan <- bulkBuf:
					atomic.AddUint64(&allBufCount, 1) // 批量写入计数，用于统计需要多少线程
				default:
					atomic.AddUint64(&dropBatchCount, 1)
				}
				bulkBuf = getBuffer() // 写入新变量
				batchBytes = 0
			}
		}
		// Flush the remaining data
		if bulkBuf.Len() > 0 {
			select {
			case bufChan <- bulkBuf:
				atomic.AddUint64(&allBufCount, 1) // 批量写入计数，用于统计需要多少线程
			default:
				atomic.AddUint64(&dropBatchCount, 1)
			}
		}
	}
	return nil
}
