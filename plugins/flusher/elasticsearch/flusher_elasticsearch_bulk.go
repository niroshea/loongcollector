package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/alibaba/ilogtail/pkg/fmtstr"
	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

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
		var bulkBuf bytes.Buffer
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
				if err := f.sendBulk(&bulkBuf); err != nil {
					logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "Bulk send failed: %s", err)
				}
				bulkBuf.Reset()
				batchBytes = 0
			}
		}
		// Flush the remaining data
		if bulkBuf.Len() > 0 {
			if err := f.sendBulk(&bulkBuf); err != nil {
				logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "Final bulk send failed: %s", err)
			}
		}
	}
	return nil
}

// func (f *FlusherElasticSearch) createBulkIndexer(indexName string) esutil.BulkIndexer {
// 	ret, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
// 		Client:        f.esClient,
// 		Index:         indexName, // 数据流名称
// 		NumWorkers:    4,
// 		FlushBytes:    12 * 1024 * 1024, // 每 12MB flush 一次
// 		FlushInterval: 30 * time.Second, // 或每 30 秒 flush
// 	})
// 	if err != nil {
// 		logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "Error creating the indexer: %s", err)
// 		return nil
// 	}
// 	return ret
// }

// var bulkIndexersMap sync.Map

// func (f *FlusherElasticSearch) getbulkIndexer(indexName string) (ret esutil.BulkIndexer) {
// 	if ret, ok := bulkIndexersMap.Load(indexName); ok && (ret != nil) {
// 		return ret.(esutil.BulkIndexer)
// 	}
// 	ret = f.createBulkIndexer(indexName)
// 	bulkIndexersMap.Store(indexName, ret)
// 	return
// }

// func (f *FlusherElasticSearch) Flush3(projectName string, logstoreName string, configName string, logGroupList []*protocol.LogGroup) error {
// 	bulkAction := "create"
// 	if f.Action != "" {
// 		bulkAction = f.Action
// 	}
// 	nowTime := time.Now().Local()
// 	for _, logGroup := range logGroupList {
// 		logger.Debug(f.context.GetRuntimeContext(), "[LogGroup] topic", logGroup.Topic, "logstore", logGroup.Category, "logcount", len(logGroup.Logs), "tags", logGroup.LogTags)
// 		serializedLogs, values, err := f.converter.ToByteStreamWithSelectedFields(logGroup, f.indexKeys)
// 		if err != nil {
// 			logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "flush elasticsearch convert log fail, error", err)
// 			return err
// 		}

// 		for index, log := range serializedLogs.([][]byte) {
// 			esIndex := &f.Index
// 			if f.isDynamicIndex {
// 				valueMap := values[index]
// 				esIndex, err = fmtstr.FormatIndex(valueMap, f.Index, uint32(nowTime.Unix()))
// 				if err != nil {
// 					logger.Error(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "ERROR flush elasticsearch format index fail, error", err)
// 					return err
// 				}
// 			}
// 			err := f.getbulkIndexer(*esIndex).Add(context.Background(), esutil.BulkIndexerItem{
// 				Action: bulkAction,
// 				Body:   bytes.NewReader(log),
// 				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, resp esutil.BulkIndexerResponseItem, err error) {
// 					if err != nil {
// 						logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "[%s] Error: %v", *esIndex, err)
// 					} else {
// 						logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "[%s] Index error: %s - %s", *esIndex, resp.Error.Type, resp.Error.Reason)
// 					}
// 				},
// 			})
// 			if err != nil {
// 				logger.Errorf(f.context.GetRuntimeContext(), "FLUSHER_FLUSH_ALARM", "getbulkIndexer Add func err: %v", err)
// 			}
// 		}
// 	}
// 	return nil
// }
