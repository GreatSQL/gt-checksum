package actions

import (
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"gt-checksum/global"
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
	// 创建源端和目标端数据的计数映射，记录每个值出现的次数
	srcCount := make(map[string]int)  // 源端数据计数
	destCount := make(map[string]int) // 目标端数据计数

	// 填充源端数据计数
	for _, v := range src {
		if v != "" {
			srcCount[v]++
		}
	}

	// 填充目标端数据计数
	for _, v := range dest {
		if v != "" {
			destCount[v]++
		}
	}

	// 计算差异
	var added, deleted []string

	// 处理需要添加的记录（考虑重复次数）
	for v, srcNum := range srcCount {
		destNum, exists := destCount[v]
		if !exists || srcNum > destNum {
			// 需要添加的数量是源端数量减去目标端数量
			addCount := srcNum - destNum
			if exists {
				// 目标端存在但数量不足，需要补充
				for i := 0; i < addCount; i++ {
					added = append(added, v) // 使用原始值，保留尾部空格
				}
			} else {
				// 目标端不存在，需要添加所有源端记录
				for i := 0; i < srcNum; i++ {
					added = append(added, v) // 使用原始值，保留尾部空格
				}
			}
		}
	}

	// 处理需要删除的记录（考虑重复次数）
	for v, destNum := range destCount {
		srcNum, exists := srcCount[v]
		if !exists || destNum > srcNum {
			// 需要删除的数量是目标端数量减去源端数量
			deleteCount := destNum - srcNum
			if exists {
				// 源端存在但数量较少，需要删除多余的
				for i := 0; i < deleteCount; i++ {
					deleted = append(deleted, v) // 使用原始值，保留尾部空格
				}
			} else {
				// 源端不存在，需要删除所有目标端记录
				for i := 0; i < destNum; i++ {
					deleted = append(deleted, v) // 使用原始值，保留尾部空格
				}
			}
		}
	}

	// 调试：记录差异数量
	global.Wlog.Debug("DEBUG_ARRCMP: src_len=%d, dest_len=%d, added_len=%d, deleted_len=%d", len(src), len(dest), len(added), len(deleted))

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
// func (csts CheckSumTypeStruct) RandomString(n int, allowedChars ...[]rune) string {
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
