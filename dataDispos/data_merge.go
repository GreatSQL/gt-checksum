package dataDispos

import (
	"fmt"
	"strconv"
	"strings"
)

// DataInfo defines buffer settings used by merge streams in data comparison workflows.
type DataInfo struct {
	ChanQueueDepth int
}

/*
两个管道的正序数据进行合并排序到一个管道
*/
func (sp *DataInfo) ChangeMerge(ch1 <-chan map[string]interface{}, ch2 <-chan map[string]interface{}) chan map[string]interface{} {
	var cc = make(chan map[string]interface{}, sp.ChanQueueDepth)
	go func() {
		var ok1, ok2 bool
		var v1, v2 map[string]interface{}
		var c1, c2 = true, true
		for {
			if c1 {
				v1, ok1 = <-ch1
			}
			if c2 {
				v2, ok2 = <-ch2
			}
			if ok1 || ok2 {
				if ok1 && ok2 {
					var v11, v22 string
					for k, _ := range v1 {
						v11 = fmt.Sprintf("%v", k)
					}
					for k, _ := range v2 {
						v22 = fmt.Sprintf("%v", k)
					}
					// 尝试将值转换为整数进行比较，以便数字类型的索引列能够正确排序
					if v11 != StreamEndMarker && v22 != StreamEndMarker {
						if v1Int, err1 := strconv.Atoi(v11); err1 == nil {
							if v2Int, err2 := strconv.Atoi(v22); err2 == nil {
								// 两个值都是整数，按数字大小比较
								if v1Int < v2Int {
									c1 = true
									c2 = false
									cc <- v1
								} else if v1Int == v2Int {
									c1 = true
									c2 = true
									cc <- v1
								} else {
									c1 = false
									c2 = true
									cc <- v2
								}
								continue
							}
						}
					}
					// 如果不是整数或者转换失败，使用字符串比较
					if strings.Compare(v11, v22) == -1 {
						c1 = true
						c2 = false
						cc <- v1
					} else if strings.Compare(v11, v22) == 0 {
						c1 = true
						c2 = true
						cc <- v1
					} else {
						c1 = false
						c2 = true
						cc <- v2
					}
				} else if ok1 && !ok2 {
					c1 = true
					c2 = false
					cc <- v1
				} else if !ok1 && ok2 {
					c1 = false
					c2 = true
					cc <- v2
				}
			} else {
				cc <- map[string]interface{}{StreamEndMarker: StreamEndValue}
				close(cc)
				break
			}
		}
	}()
	return cc
}

/*
两个管道的条件，按照指定字符进行先后顺序进行合并，先梳理delete，再梳理insert
*/
func (sp *DataInfo) Merge(ch1 <-chan map[string]interface{}, ch2 <-chan map[string]interface{}, beginST, endST string) chan map[string]interface{} {
	var cc = make(chan map[string]interface{}, sp.ChanQueueDepth)

	go func() {
		var ok1, ok2 bool
		var v1, v2 map[string]interface{}
		var c1, c2 = true, true
		for {
			if c1 {
				v1, ok1 = <-ch1
			}
			if c2 {
				v2, ok2 = <-ch2
			}
			if ok1 || ok2 {
				if ok1 && ok2 {
					var v11, v22 string
					for k, _ := range v1 {
						v11 = fmt.Sprintf("%v", k)
					}
					for k, _ := range v2 {
						v22 = fmt.Sprintf("%v", k)
					}
					if strings.HasPrefix(strings.TrimSpace(v11), beginST) && strings.HasPrefix(strings.TrimSpace(v22), beginST) {
						c1 = true
						c2 = true
						cc <- v1
						cc <- v2

					}
					if strings.HasPrefix(strings.TrimSpace(v11), beginST) && !strings.HasPrefix(strings.TrimSpace(v22), beginST) {
						c1 = true
						c2 = false
						cc <- v1
					}
					if strings.Compare(v11, v22) == -1 {
						c1 = true
						c2 = false
						cc <- v1
					} else if strings.Compare(v11, v22) == 0 {
						c1 = true
						c2 = true
						cc <- v1
					} else {
						c1 = false
						c2 = true
						cc <- v2
					}
				} else if ok1 && !ok2 {
					c1 = true
					c2 = false
					cc <- v1
				} else if !ok1 && ok2 {
					c1 = false
					c2 = true
					cc <- v2
				}
			} else {
				cc <- map[string]interface{}{StreamEndMarker: StreamEndValue}
				close(cc)
				break
			}
		}
	}()
	return cc
}
