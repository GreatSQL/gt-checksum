package actions

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/gosuri/uitable"
	"gt-checksum/inputArg"
	"strings"
)

//进度条
type Bar struct {
	percent  int64  //百分比
	cur      int64  //当前进度位置
	total    int64  //总进度
	rate     string //进度条
	graph    string //显示符号
	taskUnit string //task单位
}

type Pod struct {
	Schema, Table, IndexCol, CheckMod, Rows, Differences, CheckObject, Datafix, FuncName, Definer, ProcName, Sample, TriggerName string
}

var measuredDataPods []Pod
var differencesSchemaTable = make(map[string]string)

func CheckResultOut(m *inputArg.ConfigParameter) {
	table := uitable.New()
	table.MaxColWidth = 200
	table.RightAlign(20)

	if m.CheckObject == "struct" {
		table.AddRow("Schema", "Table ", " CheckObject ", "Differences", "Datafix")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.Differences), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	}
	if m.CheckObject == "index" {
		table.AddRow("Schema", "Table ", "CheckObject ", "Differences", "Datafix")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.Differences), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	}
	if m.CheckObject == "partitions" {
		table.AddRow("Schema", "Table ", "checkObject ", "Differences", "Datafix")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.Differences), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	}
	if m.CheckObject == "foreign" {
		table.AddRow("Schema", "Table ", "checkObject ", "Differences", "Datafix")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.Differences), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	}
	if m.CheckObject == "func" {
		table.AddRow("Schema ", "funcName ", "checkObject ", "Differences ", "Datafix ")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.FuncName), color.RedString(pod.CheckObject), color.GreenString(pod.Differences), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	}
	if m.CheckObject == "proc" {
		table.AddRow("Schema ", "procName ", "checkObject ", "Differences ", "Datafix ")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.ProcName), color.RedString(pod.CheckObject), color.GreenString(pod.Differences), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	}
	if m.CheckObject == "trigger" {
		table.AddRow("Schema ", "triggerName ", "checkObject ", "Differences ", "Datafix ")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.GreenString(pod.TriggerName), color.RedString(pod.CheckObject), color.GreenString(pod.Differences), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	}
	if m.CheckObject == "data" {
		if m.CheckMode == "count" {
			table.AddRow("Schema", "Table ", "checkObject", "checkMod", "Rows", "Differences")
			for _, pod := range measuredDataPods {
				table.AddRow(color.RedString(pod.Schema), color.GreenString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.CheckMod), color.RedString(pod.Rows), color.YellowString(pod.Differences))
			}
			fmt.Println(table)
		}
		if m.CheckMode == "sample" {

			for _, pod := range measuredDataPods {
				if pod.Sample == "" {
					table.AddRow("Schema", "Table ", "IndexCol ", "checkObject", "checkMod", "Rows", "Differences")
					table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexCol), color.YellowString(pod.CheckObject), color.BlueString(pod.CheckMod), color.BlueString(pod.Rows), color.GreenString(pod.Differences))
				} else {
					table.AddRow("Schema", "Table ", "IndexCol ", "checkObject", "checkMod", "Rows", "Samp", "Differences")
					table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexCol), color.YellowString(pod.CheckObject), color.BlueString(pod.CheckMod), color.BlueString(pod.Rows), color.RedString(pod.Sample), color.GreenString(pod.Differences))
				}
			}
			fmt.Println(table)
		}
		if m.CheckMode == "rows" {
			table.AddRow("Schema", "Table ", "IndexCol ", "checkMod", "Rows", "Differences", "Datafix")
			for _, pod := range measuredDataPods {
				var differences = pod.Differences
				for k, _ := range differencesSchemaTable {
					if k != "" {
						KI := strings.Split(k, "greatdbCheck_greatdbCheck")
						if pod.Schema == KI[0] && pod.Table == KI[1] {
							differences = "yes"
						}
					}
				}
				table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexCol), color.BlueString(pod.CheckMod), color.BlueString(pod.Rows), color.GreenString(differences), color.YellowString(pod.Datafix))
			}
			fmt.Println(table)
		}
	}
}

func (bar *Bar) NewOption(start, total int64, taskUnit string) {
	bar.cur = start
	bar.total = total
	bar.taskUnit = taskUnit
	if bar.graph == "" {
		bar.graph = "█"
	}
	bar.percent = bar.getPercent()
	for i := 0; i < int(bar.percent); i += 2 {
		bar.rate += bar.graph //初始化进度条位置
	}
}

func (bar *Bar) getPercent() int64 {
	return int64(float32(bar.cur) / float32(bar.total) * 100)
}
func (bar *Bar) NewOptionWithGraph(start, total int64, graph, taskUnit string) {
	bar.graph = graph
	bar.NewOption(start, total, taskUnit)
}

//显示进度条需要放在循环中执行，循环中展示每轮循环当前的进度状态，fmt.Pringf打印的那句话通过\r控制打印效果，在构建rate进度条时
//需要保存上一次完成的百分比，只有当百分比发生了变化，且步长变化了2，才能改变进度条长度，也可以设置进度条为100个字符，这样就不需要空值进度条的步长为2了
//每增长1%，进度条前进1格
func (bar *Bar) Play(cur int64) {
	bar.cur = cur
	last := bar.percent
	bar.percent = bar.getPercent()
	//if bar.percent != last && bar.percent%2 == 0 {
	//	bar.rate += bar.graph
	//}
	if bar.percent != last {
		bar.rate += bar.graph
	}
	//if bar.total >= 100
	//if bar.taskUnit == "task"{
	//	fmt.Printf("\r[%-21s]%3d%%  %s%8d/%d", bar.rate, bar.percent, "task:", bar.cur, bar.total)
	//}
	//if bar.taskUnit == "rows"{
	//	fmt.Printf("\r[%-21s]%3d%%  %s%8d/%d", bar.rate, bar.percent, "rows:", bar.cur, bar.total)
	//}
	fmt.Printf("\r[%-21s]%3d%%  %s%8d/%d", bar.rate, bar.percent, fmt.Sprintf("%s:", bar.taskUnit), bar.cur, bar.total)
}

//由于上面的打印没有打印换行符，因此，在进度全部结束之后（也就是跳出循环之外时），需要打印一个换行符，因此，封装了一个Finish函数，该函数纯粹的打印一个换行，表示进度条已经完成。
func (bar *Bar) Finish() {
	fmt.Println()
}
