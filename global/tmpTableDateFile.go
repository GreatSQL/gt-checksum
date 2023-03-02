package global

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

/*
   生成调度任务，查询表数据时生成的结构文件
*/
type TableDateFileStruct struct {
	FileName string
}

var TableDataFile = &TableDateFileStruct{}
var fileSync sync.Mutex

/*
   判断文件目录是否存在，不存在则创建文件目录
*/
func (tds TableDateFileStruct) TmpIsDir(dirName string, dirAction string) {
	_, exist := os.Stat(dirName)

	if dirAction == "create" {
		if os.IsNotExist(exist) {
			os.Mkdir(dirName, os.ModePerm)
		}
	}
	if dirAction == "drop" {
		if !os.IsExist(exist) {
			os.RemoveAll(dirName)
		}
	}

}

/*
   针对每个表创建一个临时文件，临时文件中的内容为每个表的查询的数据索引列条件值
*/
func (tds TableDateFileStruct) WriteFile(file *os.File, writeMapString []string) error {
	//写入数据
	write := bufio.NewWriter(file)
	//fileSync.Lock()
	//for _, v := range writeMapString {
	//	for is, i := range v {
	for is, i := range writeMapString {
		write.WriteString(i)
		if is < len(i)-1 {
			write.WriteString("/*greatdbCheckColumnSplict*/")
		}
		Wlog.Debug(fmt.Sprintf("GreatdbCheck writes data \"%s\" to file %s.", i, tds.FileName))
	}
	write.WriteString("\n")
	//}

	//把缓冲区清空，立即将最后的数据写入文件
	//fileSync.Unlock()
	write.Flush()
	Wlog.Debug(fmt.Sprintf("GreatdbCheck refreshes the data in the disk cache to the disk file and continues to drop the disk"))
	return nil
}
func (tds TableDateFileStruct) ReadFile(from, to, rownum int, fseek int64) ([]string, int, int64, error) {
	var cur_offset int64
	var aa string
	file, err := os.OpenFile(tds.FileName, os.O_RDONLY, 0666)
	if err != nil {
		Wlog.Error(fmt.Sprintf("GreatdbCheck Failed to open file %s, error message:%s", tds.FileName, err))
		return []string{}, 0, 0, err
	}
	//延迟关闭文件：在函数return前执行的程序
	defer func() {
		file.Close()
		Wlog.Debug(fmt.Sprintf("actions colse file %s file.", tds.FileName))
	}()
	file.Seek(fseek, io.SeekStart)
	//创建文件读取器
	reader := bufio.NewReader(file)
	cur_offset = fseek
	for {
		if rownum >= from && rownum <= to {
			data, err := reader.ReadBytes('\n')
			cur_offset += int64(len(data))
			line := strings.TrimSpace(string(data))
			aa += line
			if err != nil {
				if err == io.EOF {
					Wlog.Debug(fmt.Sprintf("GreatdbCheck has read the end of file %s and will stop reading data from the file.", tds.FileName))
					break
				} else {
					//读取异常，打印异常并结束
					Wlog.Error(fmt.Sprintf("GreatdbCheck Fails to read file %s, and the actions stops reading file A. Error message: %s", tds.FileName, err))
					return []string{}, 0, 0, err
				}
			}
		}
		if rownum > to {
			break
		}
		aa += ","
		rownum++
	}

	bb := strings.Split(aa, ",")
	return bb[:len(bb)-1], rownum, cur_offset, nil
}
func (tds TableDateFileStruct) FindFile(from, to int) (bool, error) {
	f, err := os.Open(tds.FileName)
	if err != nil {
		return false, err
	}
	defer f.Close()
	n := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		n++
		if n < from {
			fmt.Println("-----", string(scanner.Bytes()))
			continue
		}
		if n > to {
			break
		}
	}
	return false, scanner.Err()
}

//删除文件
func (tds TableDateFileStruct) RmFile(f ...string) error {
	for _, i := range f {
		err := os.Remove(i) //删除文件test.txt
		if err != nil {
			//如果删除失败则输出 file remove Error!
			Wlog.Error(fmt.Sprintf("GreatdbCheck Failed to delete file %s, error message: %s", i, err))
			//输出错误详细信息
			return err
		} else {
			//如果删除成功则输出 file remove OK!
			Wlog.Debug(fmt.Sprintf("GreatdbCheck Successfully delete file %s.", i))
		}
	}
	return nil
}
