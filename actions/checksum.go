package actions

import (
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"
)

type CheckSumTypeStruct struct{}

/*
   对字符串进行MD5哈希
*/
func (csts CheckSumTypeStruct) CheckMd5(data string) string {
	t := md5.New()
	io.WriteString(t, data)
	return fmt.Sprintf("%x", t.Sum(nil))
}

/*
   对字符串进行SHA1哈希
*/
func (csts CheckSumTypeStruct) CheckSha1(data string) string {
	t := sha1.New()
	io.WriteString(t, data)
	return fmt.Sprintf("%x", t.Sum(nil))
}

func (csts CheckSumTypeStruct) Arrcmap(src, dest []string) []string {
	msrc := make(map[string]byte) //按源数组建索引
	mall := make(map[string]byte) //源+目所有元素建索引
	var set []string              //交集
	//1、源数组建立map
	for _, v := range src {
		msrc[v] = 0
		mall[v] = 0
	}
	for _, v := range dest {
		l := len(mall)
		mall[v] = 1
		if l != len(mall) {
			l = len(mall)
		} else {
			set = append(set, v)
		}
	}
	return set
}

/*
   数据校验并输出差异性数据
*/
func (csts CheckSumTypeStruct) Arrcmp(src []string, dest []string) ([]string, []string) { //对比数据
	// 创建源端和目标端数据的映射
	srcMap := make(map[string]struct{}) // 源端数据映射
	destMap := make(map[string]struct{}) // 目标端数据映射
	
	// 填充源端数据映射
	for _, v := range src {
		if v != "" {
			srcMap[v] = struct{}{}
		}
	}
	
	// 填充目标端数据映射
	for _, v := range dest {
		if v != "" {
			destMap[v] = struct{}{}
		}
	}
	
	// 计算差异
	var added, deleted []string
	
	// added: 源端有但目标端没有的数据（需要从源端读取并插入到目标端）
	for v := range srcMap {
		if _, exists := destMap[v]; !exists {
			added = append(added, v)
		}
	}
	
	// deleted: 目标端有但源端没有的数据（需要从目标端删除）
	for v := range destMap {
		if _, exists := srcMap[v]; !exists {
			deleted = append(deleted, v)
		}
	}
	
	return added, deleted
}

/*
   根据两个切片找到相同的字符
*/
func (csts CheckSumTypeStruct) Arrsame(src, dest []string) string {
	msrc := make(map[string]byte) //按源数组建索引
	mall := make(map[string]byte) //源+目所有元素建索引
	var set string                //交集
	//1、源数组建立map
	for _, v := range src {
		msrc[v] = 0
		mall[v] = 0
	}
	for _, v := range dest {
		l := len(mall)
		mall[v] = 1
		if l != len(mall) {
			l = len(mall)
		} else {
			set = v
		}
	}
	return set
}

var defaultLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandomString returns a random string with a fixed length
//func (csts CheckSumTypeStruct) RandomString(n int, allowedChars ...[]rune) string {
func (csts CheckSumTypeStruct) RandomString(n int, allowedChars ...[]rune) string {
	var letters []rune
	if len(allowedChars) == 0 {
		letters = defaultLetters
	} else {
		letters = allowedChars[0]
	}
	b := make([]rune, n)
	rand.Seed(time.Now().UnixNano())
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

/*
   校验两个文件的md5值，是否一致
*/
func (csts CheckSumTypeStruct) FileMd5(f1 string) string {
	f, err := os.Open(f1)
	if err != nil {
		fmt.Println("Open", err)
		//return "", err
	}
	defer f.Close()
	md5hash := md5.New()
	if _, err = io.Copy(md5hash, f); err != nil {
		fmt.Println("Copy", err)
		//return "", err
	}
	md5Val := fmt.Sprintf("%x", md5hash.Sum(nil))
	return md5Val
}

func CheckSum() *CheckSumTypeStruct {
	return &CheckSumTypeStruct{}
}
