package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/alibaba/ilogtail/pkg/fmtstr"
	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"gopkg.in/yaml.v3"
)

var bulkconf = getConfig()

type BlukConfig struct {
	EsBlukConfig GoroutineConf `yaml:"es_bulk_config"`
}
type GoroutineConf struct {
	GoThreadNum int `yaml:"goroutine"`
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
	maxBatchBytes = 12 * 1024 * 1024 // 12MB
)

// var compressed bytes.Buffer
// var gw = gzip.NewWriter(&compressed)

func (f *FlusherElasticSearch) sendBulk(bulkBuf *bytes.Buffer) error {
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
				batchBytes = 0
			}
		}
		// Flush the remaining data
		if bulkBuf.Len() > 0 {
			// if err := f.sendBulk(&bulkBuf); err != nil {
			// 	logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "Final bulk send failed: %s", err)
			// }
			bufChan <- bulkBuf
		}
	}
	return nil
}
