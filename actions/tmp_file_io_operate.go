package actions

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"gt-checksum/global"
	"io"
	"math"
	"os"
	"strings"
	"sync"
)

/*
	文件io操作，并行写入和读取文件
*/

var (
	mutex sync.Mutex
)

type FileOperate struct {
	File     *os.File
	BufSize  int
	SqlType  string
	fileName string
}

/*
	文件并发写入
*/
func (f FileOperate) ConcurrencyWriteFile(writeString []string) ([]string, error) {
	var (
		c        string
		md5Slice []string
		event    string
		vlog     string
	)
	bufWriter := bufio.NewWriterSize(f.File, f.BufSize)
	event = fmt.Sprintf("[%s]", "write_file")
	for _, i := range writeString {
		sum := md5.Sum([]byte(i))
		sumS := hex.EncodeToString(sum[:])
		md5Slice = append(md5Slice, sumS)
		if f.SqlType == "sql" {
			c = fmt.Sprintf("%s \n", i)
		} else {
			c = fmt.Sprintf("%s %s %s \n", sumS, f.SqlType, i)
		}
		mutex.Lock()
		vlog = fmt.Sprintf("() %s Start to write data to file %s, the written content is {%v}", event, f.fileName, c)
		global.Wlog.Debug(vlog)
		wc, err := bufWriter.WriteString(c)
		bufWriter.Flush()
		if err != nil {
			vlog = fmt.Sprintf("() %s File %s failed to write content %s, the error message is {%v}", event, f.fileName, c, err)
			global.Wlog.Error(vlog)
			return nil, err
		}
		if wc != len(c) {
			vlog = fmt.Sprintf("() %s The number of written bytes of file %s does not match the number of successful bytes, the number of written bytes is {%v}, and the number of successful bytes is {%v}", event, f.fileName, len(c), wc)
			global.Wlog.Error(vlog)
			return nil, err
		}
		mutex.Unlock()
		vlog = fmt.Sprintf("() %s The data in file %s is successfully written.", event, f.fileName)
		global.Wlog.Debug(vlog)
	}

	return md5Slice, nil
}

func ProcessChunk(chunk []byte, linesPool *sync.Pool, stringPool *sync.Pool, m map[string]string, c chan<- map[string]string) {
	var (
		wg2 sync.WaitGroup
	)
	logs := stringPool.Get().(string)
	logs = string(chunk)
	linesPool.Put(chunk)
	logsSlice := strings.Split(logs, "\n")
	stringPool.Put(logs)
	chunkSize := 300
	n := len(logsSlice)
	noOfThread := n / chunkSize
	if n%chunkSize != 0 {
		noOfThread++
	}
	for i := 0; i < (noOfThread); i++ {
		wg2.Add(1)
		go func(s int, e int) {
			defer wg2.Done() //to avaoid deadlocks
			for i := s; i < e; i++ {
				text := logsSlice[i]
				if len(text) == 0 {
					continue
				}
				logSlice := strings.SplitN(text, " ", 3)
				md5Sum := logSlice[0]
				sqlType := logSlice[1]
				if v, ok := m[md5Sum]; ok && v == sqlType {
					//fmt.Println(logSlice[2])
					c <- map[string]string{logSlice[2]: v}
				}
			}
		}(i*chunkSize, int(math.Min(float64((i+1)*chunkSize), float64(len(logsSlice)))))
	}
	wg2.Wait()
	logsSlice = nil
}

//func ProcessStatus(chunk []byte, linesPool *sync.Pool, stringPool *sync.Pool, m string) bool {
//
//	return exist
//}

func (f FileOperate) ConcurrencyReadFile(F map[string]string, c chan map[string]string) error {
	var err error
	//sync pools to reuse the memory and decrease the preassure on //Garbage Collector
	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, 500*1024)
		return lines
	}}
	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}
	//slicePool := sync.Pool{New: func() interface{} {
	//	lines := make([]string, 100)
	//	return lines
	//}}
	file, _ := os.Open(f.fileName)
	bufReader := bufio.NewReader(file)
	var wg sync.WaitGroup //wait group to keep track off all threads
	for {
		var n int
		var nextUntillNewline []byte
		buf := linesPool.Get().([]byte)
		n, err = bufReader.Read(buf)
		buf = buf[:n]
		if n == 0 {
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("--error--", err)
				break
			}
			close(c)
			return err
		}
		nextUntillNewline, err = bufReader.ReadBytes('\n') //read entire line
		if err != io.EOF {
			buf = append(buf, nextUntillNewline...)
		}
		wg.Add(1)
		go func() {
			//process each chunk concurrently
			//start -> log start time, end -> log end time
			ProcessChunk(buf, &linesPool, &stringPool, F, c)
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

//写文件内容之前需要判断一下文件内容中是否存在，不存在则写入
func (f FileOperate) ReadWriteFile(F ...interface{}) ([]string, []string) {
	var err error
	var exist, noexit []string
	//sync pools to reuse the memory and decrease the preassure on //Garbage Collector
	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, 500*1024)
		return lines
	}}
	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}
	//slicePool := sync.Pool{New: func() interface{} {
	//	lines := make([]string, 100)
	//	return lines
	//}}
	fp, _ := os.Open(f.fileName)
	bufReader := bufio.NewReader(fp)
	defer fp.Close()
	var wg sync.WaitGroup //wait group to keep track off all threads
	for {
		var n int
		var nextUntillNewline []byte
		buf := linesPool.Get().([]byte)
		n, err = bufReader.Read(buf)
		buf = buf[:n]
		if n == 0 {
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("--error--", err)
				break
			}
			return exist, noexit
		}
		nextUntillNewline, err = bufReader.ReadBytes('\n') //read entire line
		if err != io.EOF {
			buf = append(buf, nextUntillNewline...)
		}
		wg.Add(1)
		go func() {
			//process each chunk concurrently
			//start -> log start time, end -> log end time
			var (
				wg2 sync.WaitGroup
			)
			logs := stringPool.Get().(string)
			logs = string(buf)
			linesPool.Put(buf)
			logsSlice := strings.Split(logs, "\n")
			stringPool.Put(logs)
			chunkSize := 300
			n := len(logsSlice)
			noOfThread := n / chunkSize
			if n%chunkSize != 0 {
				noOfThread++
			}
			for i := 0; i < (noOfThread); i++ {
				wg2.Add(1)
				go func(s int, e int) {
					defer wg2.Done() //to avaoid deadlocks
					for i := s; i < e; i++ {
						text := logsSlice[i]
						if len(text) == 0 {
							continue
						}
						logSlice := strings.SplitN(text, " ", 3)
						exist, noexit = nil, nil
						for _, vv := range F {
							for _, vvi := range vv.([]map[string]string) {
								if strings.Split(logSlice[0], ",")[0] == vvi["columnName"] {
									fmt.Println("del--:", fmt.Sprintf("%s,%s)", vvi["columnName"], vvi["count"]))
									exist = append(exist, fmt.Sprintf("%s,%s)", vvi["columnName"], vvi["count"]))
								} else {
									fmt.Println("add--:", fmt.Sprintf("%s,%s)", vvi["columnName"], vvi["count"]))
									noexit = append(noexit, fmt.Sprintf("%s,%s)", vvi["columnName"], vvi["count"]))
								}
							}

						}
					}
				}(i*chunkSize, int(math.Min(float64((i+1)*chunkSize), float64(len(logsSlice)))))
			}
			wg2.Wait()
			logsSlice = nil
			wg.Done()
		}()
	}
	wg.Wait()
	return exist, noexit
}
