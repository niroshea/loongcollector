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
	"gopkg.in/yaml.v3"
)

var (
	totalDuration int64  // sendBulk函数消耗时间，累计，单位纳秒 -- 计算处理耗时
	callCount     int64  // 调用 sendBulk 次数 -- 计算处理耗时
	allBufCount   uint64 // buf 写入channel 计数 -- 计算写入速率为 W（条/秒）
)

func init() {
	go performanceLog()
}

func performanceLog() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	var old_totalDuration, old_callCount int64
	var old_allBufCount uint64
	for range ticker.C {
		new_totalDuration, new_callCount, new_allBufCount :=
			atomic.LoadInt64(&totalDuration), atomic.LoadInt64(&callCount), atomic.LoadUint64(&allBufCount)

		dura_60 := new_totalDuration - old_totalDuration
		count_60 := new_callCount - old_callCount
		allBufCount_60 := new_allBufCount - old_allBufCount

		// 每次请求耗时
		var sendBulkDura int64
		if count_60 < 1 {
			sendBulkDura = 0
		} else {
			sendBulkDura = dura_60 / count_60
		}
		log.Printf("---- buf channel already write [ %d ] and current write rate: [ %d/s ], sendBulk func duration: [ %s ], Min goroutine is [ %d ].\n",
			new_allBufCount,
			allBufCount_60/60,
			time.Duration(sendBulkDura).String(),
			int64(allBufCount_60)*sendBulkDura/60/1e9,
		)
		old_totalDuration, old_callCount, old_allBufCount = new_totalDuration, new_callCount, new_allBufCount
	}
}

var bulkconf = getConfig()

type BlukConfig struct {
	EsBlukConfig GoroutineConf `yaml:"es_bulk_config"`
}
type GoroutineConf struct {
	GoThreadNum int `yaml:"goroutine"`
	BatchSizeMB int `yaml:"batch_size_mb"`
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
		return new(bytes.Buffer)
	},
}

func getBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putBuffer(buf *bytes.Buffer) {
	bufferPool.Put(buf)
}

var bufChan = make(chan *bytes.Buffer, 100)

func (f *FlusherElasticSearch) handleBufChan() {
	goThreadNum := bulkconf.EsBlukConfig.GoThreadNum
	if goThreadNum < 2 {
		goThreadNum = 10
	}
	log.Println("es bulk goroutine number:", goThreadNum)
	for range goThreadNum {
		go func() {
			for v := range bufChan {
				if err := f.sendBulk(v); err != nil {
					logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "Bulk send failed: %s", err)
				}
				putBuffer(v)
			}
		}()
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
	log.Println("es bulk size(MB):", bsize)
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
		for index, log := range serializedLogs.([][]byte) {
			esIndex := &f.Index
			if f.isDynamicIndex {
				valueMap := values[index]
				esIndex, err = fmtstr.FormatIndex(valueMap, f.Index, uint32(nowTime.Unix()))
				if err != nil {
					logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "ERROR flush elasticsearch format index fail, error", err)
					return err
				}
			}
			meta := []byte(`{"` + bulkAction + `": {"_index": "` + *esIndex + `"}}` + "\n")
			log = append(log, "\n"...)
			logLen := len(meta) + len(log)
			//
			bulkBuf.Grow(logLen)
			bulkBuf.Write(meta)
			bulkBuf.Write(log)
			//
			batchBytes += logLen
			if batchBytes >= maxBatchBytes {
				// if err := f.sendBulk(&bulkBuf); err != nil {
				// 	logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "Bulk send failed: %s", err)
				// }
				// bulkBuf.Reset()
				bufChan <- bulkBuf
				atomic.AddUint64(&allBufCount, 1) // 批量写入计数，用于统计需要多少线程
				batchBytes = 0
			}
		}
		// Flush the remaining data
		if bulkBuf.Len() > 0 {
			// if err := f.sendBulk(&bulkBuf); err != nil {
			// 	logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "Final bulk send failed: %s", err)
			// }
			bufChan <- bulkBuf
			atomic.AddUint64(&allBufCount, 1) // 批量写入计数，用于统计需要多少线程
		}
	}
	return nil
}
