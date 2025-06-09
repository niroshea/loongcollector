package rename

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var projEsConfig = topicConfig()
var allErrInfo strings.Builder

type ProjConfig struct {
	Project map[string]map[string]ProjLogGroup `yaml:"project"`
}

type ProjLogGroup struct {
	Retention int      `yaml:"retention"`
	Apps      []string `yaml:"apps"`
}

// 索引模板名称格式 IndexPatterns(ismDays, appName) + _index_template  .ds-convergence-7-days-2025.06.06-000236
func IndexPatterns(project string, retention int) string { //convergence-7-days
	return project + "-" + strconv.Itoa(retention) + "-days"
}

var projectRegxMap = make(map[string]*regexp.Regexp)
var projectAppMap = make(map[string](map[string]string))

const (
	DefaultProject = "convergence"
	DefaultMapKey  = "__default_key__"
	TimeFormat     = "2006-01-02_15-04-05"
	TopicKey       = "topic"
	NamespaceKey   = "_namespace_"
	ContainerKey   = "_container_name_"
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
				tmpAppMap[app] = IndexPatterns(projectName, group.Retention)
			}
		}
		tmpAppMap[DefaultMapKey] = IndexPatterns(projectName, 7)
		projectAppMap[projectName] = tmpAppMap
	}
	tTags2()
}

func genTopicName(namespace, container interface{}) string {
	namespaceStr, containerStr := namespace.(string), container.(string)
	var project = DefaultProject
	for proj := range projectRegxMap {
		if projectRegxMap[proj].MatchString(namespaceStr) {
			project = proj
			break
		}
	}
	if index, ok := projectAppMap[project][containerStr]; ok {
		return index
	}
	return projectAppMap[project][DefaultMapKey]
}

// 读取配置文件信息
func topicConfig() *ProjConfig {
	var config ProjConfig
	data, err := os.ReadFile("/usr/local/loongcollector/conf/continuous_pipeline_config/local/" + pluginType + ".yaml")
	if err != nil {
		allErrInfo.WriteString(err.Error() + "\n")
		return nil
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		allErrInfo.WriteString(err.Error() + "\n")
		return nil
	}
	return &config
}

// func tTags() {
// 	timeNowStr := time.Now().Format(TimeFormat)
// 	os.WriteFile("/usr/local/loongcollector/shebinbin_"+timeNowStr+".log", []byte("ok\n"), 0755)
// }

func tTags2() {
	os.WriteFile("/usr/local/loongcollector/shebinbin_1_"+time.Now().Format(TimeFormat)+".log", []byte(allErrInfo.String()), 0755)
}
