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
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"gopkg.in/yaml.v3"
)

var (
	sendBulkTotalDura int64 // sendBulk函数消耗时间，累计，单位纳秒 -- 计算处理耗时
	sendBulkCallCount int64 // 调用 sendBulk 次数 -- 计算处理耗时
	//
	allBufCount uint64 // buf 写入channel 计数 -- 计算写入速率为 W（条/秒）
	//
	// logBufferLen     int64
	// esWriteBufferLen int64
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
			atomic.LoadInt64(&sendBulkTotalDura), atomic.LoadInt64(&sendBulkCallCount), atomic.LoadUint64(&allBufCount)

		dura_60 := new_totalDuration - old_totalDuration
		count_60 := new_callCount - old_callCount
		//
		allBufCount_60 := new_allBufCount - old_allBufCount
		// 每次请求耗时
		var sendBulkDura int64
		if count_60 > 0 {
			sendBulkDura = dura_60 / count_60
		}
		log.Printf("--- INFO buf chan write [ %d ] - write rate: [ %.3f/s ] - bulk write dura [ %s ] "+
			"- chan read rate [ %.3f/s ] - bufChan[ %d/%d ] - min thread [ %.3f ].\n",
			new_allBufCount,
			float64(allBufCount_60)/60,
			time.Duration(sendBulkDura).String(),
			float64(count_60)/60,
			len(bufChan),
			cap(bufChan),
			float64(allBufCount_60)*float64(sendBulkDura)/60/1e9,
		)
		old_totalDuration, old_callCount, old_allBufCount = new_totalDuration, new_callCount, new_allBufCount
	}
}

var bulkconf = getConfig()
var maxBatchBytes = getMaxBatchBytes()

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
		ret = 500
	}
	log.Println("es buf chan size:", ret)
	return
}

func getGoroutineSize() (ret int) {
	ret = bulkconf.EsBlukConfig.GoThreadNum
	if ret < 2 {
		ret = 15
	}
	log.Println("es bulk goroutine size:", ret)
	return
}

func getMaxBatchBytes() int {
	bsize := bulkconf.EsBlukConfig.BatchSizeMB
	if bsize < 1 {
		bsize = 5
	}
	log.Println("es bulk size(MB):", bsize)
	return bsize * MB
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

var bufChan = make(chan *bytes.Buffer, getBufChanSize())

const (
	MB int = 1024 * 1024 // 1MB
)

var bufferPool = sync.Pool{
	New: func() any {
		// 预先分配容量，避免每次扩容
		return bytes.NewBuffer(make([]byte, 0, maxBatchBytes))
	},
}

func GetBuffer() *bytes.Buffer {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset() // 清空旧数据
	return buf
}

func PutBuffer(buf *bytes.Buffer) {
	// 超过最大大小的 buffer 不放回池中，防止长期占用过多内存
	if buf.Cap() > maxBatchBytes+1*MB {
		log.Println("--- WARN 释放buffer")
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}

var goroutineSize = getGoroutineSize()

func (f *FlusherElasticSearch) handleBufChan() {
	for i := 0; i < goroutineSize; i++ {
		for tbuf := range bufChan {
			if err := f.sendBulk(tbuf); err != nil {
				log.Println("--- ERROR Bulk send failed:", err.Error())
			}
			PutBuffer(tbuf)
		}
	}
}

func (f *FlusherElasticSearch) sendBulk(bulkBuf *bytes.Buffer) error {
	start := time.Now()
	defer func() {
		atomic.AddInt64(&sendBulkTotalDura, time.Since(start).Nanoseconds())
		atomic.AddInt64(&sendBulkCallCount, 1)
	}()
	req := esapi.BulkRequest{
		Body: bytes.NewReader(bulkBuf.Bytes()),
		Header: http.Header{
			"Content-Encoding": {"gzip"},
			"Content-Type":     {"application/json"},
		},
	}
	res, err := req.Do(context.Background(), f.esClient)
	if err != nil {
		log.Println("--- ERROR FLUSHER_FLUSH_ALARM", "flush elasticsearch request fail, error", err)
		return err
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 && res.StatusCode <= 499 {
		log.Println("--- ERROR FLUSHER_FLUSH_ALARM", "flush elasticsearch request client error", res)
		return fmt.Errorf("err status returned: %v", res.Status())
	} else if res.StatusCode >= 500 && res.StatusCode <= 599 {
		log.Println("--- ERROR FLUSHER_FLUSH_ALARM", "flush elasticsearch request server error", res)
		return fmt.Errorf("err status returned: %v", res.Status())
	}
	return nil
}

func (f *FlusherElasticSearch) Flush(projectName string, logstoreName string, configName string, logGroupList []*protocol.LogGroup) error {
	bulkAction := "create"
	if f.Action != "" {
		bulkAction = f.Action
	}
	tmpBuf := GetBuffer()
	var byteTmpLen int
	nowTime := time.Now().Local()
	for _, logGroup := range logGroupList {
		//logger.Debug(f.context.GetRuntimeContext(), "[LogGroup] topic", logGroup.Topic, "logstore", logGroup.Category, "logcount", len(logGroup.Logs), "tags", logGroup.LogTags)
		serializedLogs, values, err := f.converter.ToByteStreamWithSelectedFields(logGroup, f.indexKeys)
		if err != nil {
			log.Println("--- ERROR FLUSHER_FLUSH_ALARM", "flush elasticsearch convert log fail, error", err)
			return err
		}
		for idx, logData := range serializedLogs.([][]byte) {
			esIndex := &f.Index
			if f.isDynamicIndex {
				valueMap := values[idx]
				esIndex, err = fmtstr.FormatIndex(valueMap, f.Index, uint32(nowTime.Unix()))
				if err != nil {
					log.Println("--- ERROR FLUSHER_FLUSH_ALARM", "ERROR flush elasticsearch format index fail, error", err)
					return err
				}
			}
			meta := []byte(`{"` + bulkAction + `": {"_index": "` + *esIndex + `"}}` + "\n")
			logData = append(logData, "\n"...)
			//
			byteTmpLen += len(meta) + len(logData)
			//
			tmpBuf.Write(meta)
			tmpBuf.Write(logData)
			//
			if byteTmpLen >= maxBatchBytes {
				select {
				case bufChan <- tmpBuf:
					atomic.AddUint64(&allBufCount, 1)
				default:
					log.Println("--- WARN bufChan full, drop batch")
					PutBuffer(tmpBuf)
				}
				tmpBuf = GetBuffer() // --- 获取新的 buffer，开始新一轮累积
			}
		}
	}
	if tmpBuf.Len() > 0 {
		select {
		case bufChan <- tmpBuf:
			atomic.AddUint64(&allBufCount, 1)
		default:
			log.Println("--- WARN bufChan full, drop batch")
			PutBuffer(tmpBuf)
		}
	}
	return nil
}
