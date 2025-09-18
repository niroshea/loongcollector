package rename

import (
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

var projEsConfig = topicConfig()

type ProjConfig struct {
	Project map[string]map[string]ProjLogGroup `yaml:"project"`
}

type ProjLogGroup struct {
	Retention int      `yaml:"retention"`
	Apps      []string `yaml:"apps"`
}

// 索引模板名称格式 IndexPatterns(ismDays, appName) + _index_template  .ds-convergence-7-days-2025.06.06-000236
func IndexPatterns(project string, retention string) string { //convergence-7-days
	return project + "-" + retention + "-days"
}

var projectRegxMap = make(map[string]*regexp.Regexp)
var projectAppMap = make(map[string](map[string]string))

const (
	DefaultProject = "convergence"
	DefaultMapKey  = "__default_key__"
	TimeFormat     = "2006-01-02_15-04-05"
	_TopicKey      = "topic"
	_NamespaceKey  = "_namespace_"
	_ContainerKey  = "_container_name_"
	_ContentKey    = "content"
)

func init() {
	// 示例输出：打印所有项目和日志组的信息
	for projectName, logGroups := range projEsConfig.Project {
		if projectName != DefaultProject {
			projectRegxMap[projectName] = regexp.MustCompile(`^` + projectName + `-\S+`)
		}
		tmpAppMap := make(map[string]string)
		for _, group := range logGroups {
			for _, app := range group.Apps {
				if group.Retention < 7 {
					group.Retention = 7
				}
				tmpAppMap[app] = IndexPatterns(projectName, strconv.Itoa(group.Retention))
			}
		}
		tmpAppMap[DefaultMapKey] = IndexPatterns(projectName, "7")
		projectAppMap[projectName] = tmpAppMap
	}
	go performanceLog()
	go clearAggMap()
}

func genTopicName(namespace, container interface{}, retention string) string {
	namespaceStr, containerStr := namespace.(string), container.(string)
	var project = DefaultProject
	for proj := range projectRegxMap {
		if projectRegxMap[proj].MatchString(namespaceStr) {
			project = proj
			break
		}
	}
	if project == DefaultProject && retention != "" {
		return IndexPatterns(DefaultProject, retention)
	}
	if index, ok := projectAppMap[project][containerStr]; ok {
		return index
	}
	return projectAppMap[project][DefaultMapKey]
}

// 读取配置文件信息
func topicConfig() *ProjConfig {
	var config ProjConfig
	data, err := os.ReadFile("/usr/local/loongcollector/conf/continuous_pipeline_config/local/hik_processor_rename.yaml")
	if err != nil {
		log.Fatalln(err)
		return nil
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalln(err)
		return nil
	}
	return &config
}

const (
	_LogTruncateLen int    = 256 * 1024 // 256KB
	_SuffixTruncate string = "...Max_256KB_CutOff"
	//
	_LevelKey string = "level"
)

// truncateUTF8Safe 截取 UTF-8 字符串的前 n 个字节，确保不截断字符。
func truncateUTF8Safe(s string) (string, int) {
	sLen := len(s)
	if sLen <= _LogTruncateLen {
		return s, sLen
	}
	end := _LogTruncateLen
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end] + _SuffixTruncate, sLen
}

func getLogLevel(logContent string) string {
	if level, ok := levelMap[levelRegex.FindString(shortLog(logContent))]; ok {
		return level
	}
	return "NULL"
}

func genIndexRetention(logContent string) string {
	return fastMatchFromEnd(shortLog200(logContent))
}

func shortLog(s string) string {
	if len(s) <= 50 {
		return s
	}
	end := 50
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end]
}

func shortLog200(s string) string {
	if len(s) <= 200 {
		return s
	}
	end := 200
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end]
}

func fastMatchFromEnd(s string) string {
	sLen := len(s)
	if sLen < 60 {
		return ""
	}
	for i := sLen - 5; i >= 55; i-- {
		if s[i] == ' ' && s[i+4] == ' ' && s[i+1] == '[' && s[i+3] == ']' {
			switch s[i+2] {
			case 'M':
				return "14"
			case 'T':
				return "21"
			case 'L':
				return "28"
			}
		}
	}
	return ""
}

var levelRegex *regexp.Regexp

func init() {
	var levelRegexArr []string
	for k := range levelMap {
		levelRegexArr = append(levelRegexArr, k)
	}

	levelRegex = regexp.MustCompile(`\b(` + strings.Join(levelRegexArr, "|") + `)\b`)
}

var levelMap = map[string]string{
	"INFO": "INFO",
	"INF":  "INFO",
	"info": "INFO",
	//
	"WARN": "WARN",
	"WRN":  "WARN",
	"warn": "WARN",
	//
	"ERROR": "ERROR",
	"ERR":   "ERROR",
	"error": "ERROR",
	//
	"DEBUG": "DEBUG",
	"DBG":   "DEBUG",
	"debug": "DEBUG",
}

var logBytesAggMap = NewAppSizeAggregator()

func performanceLog() {
	ticker := time.NewTicker(13 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		bytes_snapshot := logBytesAggMap.Snapshot()
		bytesWrittenVec.Reset()
		countWrittenVec.Reset()
		for appKey, value := range bytes_snapshot {
			if app, ns, ok := getAppNs(appKey); ok {
				bytesWrittenVec.WithLabelValues(app, ns).Set(float64(value[0]))
				countWrittenVec.WithLabelValues(app, ns).Set(float64(value[1]))
			}
		}
	}
}

func clearAggMap() { // 间隔清理数据，如果间隔内 key 没有产生数据的话
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	old_snapshot := make(map[string][]uint64)
	for range ticker.C {
		new_snapshot := logBytesAggMap.Snapshot()
		for appKey, tSize := range new_snapshot {
			newBsize := tSize[0]
			var oldBsize uint64
			if len(old_snapshot[appKey]) > 0 {
				oldBsize = old_snapshot[appKey][0]
			}
			if newBsize-oldBsize < 1 {
				logBytesAggMap.Delete(appKey)
			}
		}
		old_snapshot = new_snapshot
	}
}

func getAppNs(key string) (app, ns string, ok bool) {
	xlist := strings.Fields(key)
	if len(xlist) != 2 {
		return
	}
	return xlist[0], xlist[1], true
}

var bytesWrittenVec = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "log_convergence_app_bytes_written_total",
		Help: "Total number of log bytes written by app",
	},
	[]string{"log_container", "log_namespace"},
)

var countWrittenVec = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "log_convergence_app_count_written_total",
		Help: "Total count of logs written by app",
	},
	[]string{"log_container", "log_namespace"},
)

func init() {
	// 创建一个新的注册器，不注册默认的 Go 和进程指标
	reg := prometheus.NewRegistry()

	reg.MustRegister(bytesWrittenVec, countWrittenVec)

	// 暴露自定义注册器的 /metrics 接口
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Println("Listening on :8080/metrics without default metrics")
	go func() {
		err := http.ListenAndServe(":8080", nil)
		if err != nil {
			log.Println(err)
		}
	}()
}
