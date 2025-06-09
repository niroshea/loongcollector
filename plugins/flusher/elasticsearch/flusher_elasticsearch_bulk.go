package elasticsearch

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/alibaba/ilogtail/pkg/fmtstr"
	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/protocol"
)

const (
	maxBatchBytes = 12 * 1024 * 1024 // 12MB
)

func (f *FlusherElasticSearch) sendBulk(bulkBuf *bytes.Buffer) error {
	var compressed bytes.Buffer
	gw := gzip.NewWriter(&compressed)
	if _, err := io.Copy(gw, bulkBuf); err != nil {
		return err
	}
	gw.Close()

	res, err := f.esClient.Bulk(bytes.NewReader(compressed.Bytes()),
		f.esClient.Bulk.WithContext(context.Background()),
		f.esClient.Bulk.WithHeader(map[string]string{
			"Content-Encoding": "gzip",
		}),
	)
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk error response: %s", res.String())
	}
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
