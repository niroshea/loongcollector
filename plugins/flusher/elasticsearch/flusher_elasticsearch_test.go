// Copyright 2023 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package elasticsearch

import (
	"fmt"
	"math/rand"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGetIndexKeys(t *testing.T) {
	Convey("Given an empty index", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should not be extracted correctly", func() {
				So(err, ShouldNotBeNil)
				So(isDynamicIndex, ShouldBeFalse)
				So(keys, ShouldBeNil)
			})
		})
	})
	Convey("Given a normal index", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "normal_index",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeFalse)
				So(len(keys), ShouldEqual, 0)
			})
		})
	})
	Convey("Given a variable index", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "index_${var}",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeFalse)
				So(len(keys), ShouldEqual, 0)
			})
		})
	})
	Convey("Given a field dynamic index expression", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "index_%{content.field}",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeTrue)
				So(len(keys), ShouldEqual, 1)
				So(keys[0], ShouldEqual, "content.field")
			})
		})
	})
	Convey("Given a timestamp dynamic index expression", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "index_%{+yyyyMM}",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeTrue)
				So(len(keys), ShouldEqual, 0)
			})
		})
	})
	Convey("Given a composite dynamic index expression", t, func() {
		flusher := &FlusherElasticSearch{
			Index: "index_%{content.field}_%{tag.host.ip}_%{+yyyyMMdd}",
		}
		Convey("When getIndexKeys is called", func() {
			keys, isDynamicIndex, err := flusher.getIndexKeys()
			Convey("Then the keys should be extracted correctly", func() {
				So(err, ShouldBeNil)
				So(isDynamicIndex, ShouldBeTrue)
				So(len(keys), ShouldEqual, 2)
				So(keys[0], ShouldEqual, "content.field")
				So(keys[1], ShouldEqual, "tag.host.ip")
			})
		})
	})
}

func randomApp(apps []string) string {
	return apps[rand.Intn(len(apps))]
}

func generateAppList(n int) []string {
	apps := make([]string, n)
	for i := 0; i < n; i++ {
		apps[i] = fmt.Sprintf("app-%04d", i)
	}
	return apps
}

func BenchmarkAppSizeAggregator_Add(b *testing.B) {
	agg := NewAppSizeAggregator()
	apps := generateAppList(200)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			app := randomApp(apps)
			agg.Add(app, 300)
		}
	})
}

func BenchmarkAppSizeAggregator_Get(b *testing.B) {
	agg := NewAppSizeAggregator()
	apps := generateAppList(200)
	// 先预写入，确保读取时有数据
	for i := 0; i < 100000; i++ {
		agg.Add(randomApp(apps), 300)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			app := randomApp(apps)
			_ = agg.Get(app)
		}
	})
}
