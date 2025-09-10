package actions

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/gosuri/uitable"
	"gt-checksum/inputArg"
	"strings"
	"time"
)

//进度条
type Bar struct {
	percent      int64  //百分比
	cur          int64  //当前进度位置
	total        int64  //总进度
	rate         string //进度条
	graph        string //显示符号
	taskUnit     string //task单位
	lastUpdate   int64  //上次更新时间戳（毫秒）
	updateInterval int64 //更新间隔（毫秒）
}

type Pod struct {
	Schema, Table, IndexColumn, CheckMode, Rows, DIFFS, CheckObject, Datafix, FuncName, Definer, ProcName, Sample, TriggerName string
}

var measuredDataPods []Pod
var differencesSchemaTable = make(map[string]string)

func CheckResultOut(m *inputArg.ConfigParameter) {
	table := uitable.New()
	table.MaxColWidth = 200
	table.RightAlign(20)

	switch m.SecondaryL.RulesV.CheckObject {
	case "struct":
		table.AddRow("Schema", "Table", " CheckObject ", "Diffs", "Datafix")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	case "index":
		table.AddRow("Schema", "Table", "CheckObject ", "Diffs", "Datafix")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	case "partitions":
		table.AddRow("Schema", "Table", "checkObject ", "Diffs", "Datafix")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	case "foreign":
		table.AddRow("Schema", "Table", "checkObject ", "Diffs", "Datafix")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	case "func":
		table.AddRow("Schema ", "funcName ", "checkObject ", "DIFFS ", "Datafix ")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.FuncName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	case "proc":
		table.AddRow("Schema ", "procName ", "checkObject ", "DIFFS ", "Datafix ")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.ProcName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	case "trigger":
		table.AddRow("Schema ", "triggerName ", "checkObject ", "Diffs ", "Datafix ")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.GreenString(pod.TriggerName), color.RedString(pod.CheckObject), color.GreenString(pod.DIFFS), color.YellowString(pod.Datafix))
		}
		fmt.Println(table)
	case "data":
		switch m.SecondaryL.RulesV.CheckMode {
		case "count":
			table.AddRow("Schema", "Table", "checkObject", "checkMode", "Rows", "Diffs")
			for _, pod := range measuredDataPods {
				table.AddRow(color.RedString(pod.Schema), color.GreenString(pod.Table), color.RedString(pod.CheckObject), color.GreenString(pod.CheckMode), color.RedString(pod.Rows), color.YellowString(pod.DIFFS))
			}
			fmt.Println(table)
		case "sample":
			for _, pod := range measuredDataPods {
				if pod.Sample == "" {
					table.AddRow("Schema", "Table", "IndexColumn", "checkObject", "checkMode", "Rows", "Diffs")
					table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.YellowString(pod.CheckObject), color.BlueString(pod.CheckMode), color.BlueString(pod.Rows), color.GreenString(pod.DIFFS))
				} else {
					table.AddRow("Schema", "Table", "IndexColumn", "checkObject", "checkMode", "Rows", "Samp", "Diffs")
					table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.YellowString(pod.CheckObject), color.BlueString(pod.CheckMode), color.BlueString(pod.Rows), color.RedString(pod.Sample), color.GreenString(pod.DIFFS))
				}
			}
			fmt.Println(table)
		case "rows":
			table.AddRow("Schema", "Table", "IndexColumn", "checkMode", "Rows", "Diffs", "Datafix")
			for _, pod := range measuredDataPods {
				var differences = pod.DIFFS
				for k, _ := range differencesSchemaTable {
					if k != "" {
						KI := strings.Split(k, "greatdbCheck_greatdbCheck")
						if pod.Schema == KI[0] && pod.Table == KI[1] {
							differences = "yes"
						}
					}
				}
				table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.IndexColumn), color.BlueString(pod.CheckMode), color.BlueString(pod.Rows), color.GreenString(differences), color.YellowString(pod.Datafix))
			}
			fmt.Println(table)
		}
	}
}

func (bar *Bar) NewOption(start, total int64, taskUnit string) {
	bar.cur = start
	bar.total = total
	bar.taskUnit = taskUnit
	bar.updateInterval = 100 // 调整为100毫秒更新一次，使进度条更流畅
	if bar.graph == "" {
		bar.graph = "█"
	}
	bar.percent = bar.getPercent()
	// 计算进度条长度：每个█字符代表约3.33%的进度（100% / 30个字符）
	progressBars := int(float64(bar.percent) * 30 / 100)
	for i := 0; i < progressBars; i++ {
		bar.rate += bar.graph //初始化进度条位置
	}
}

func (bar *Bar) getPercent() int64 {
	if bar.total == 0 {
		return 0
	}
	percent := int64(float32(bar.cur) / float32(bar.total) * 100)
	// 确保百分比不超过100%
	if percent > 100 {
		return 100
	}
	return percent
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
	
	currentTime := time.Now().UnixMilli()
	
	// 强制在进度完成时更新进度条
	if bar.percent == 100 || bar.cur == bar.total {
		// 补全进度极条到100% (30个█字符)
		for len(bar.rate) < 30 {
			bar.rate += bar.graph
		}
		bar.percent = 100
		fmt.Printf("\r\033[K[%-30s]%3d%%  %s%5d/100", bar.rate, bar.percent, fmt.Sprintf("%s:", bar.taskUnit), bar.percent)
	} else if bar.percent != last && (currentTime - bar.lastUpdate) >= bar.updateInterval {
		// 只在百分比变化且达到更新时间间隔时才更新进度条
		// 计算当前应该显示的进度条长度
		progressBars := int(bar.percent) / 5
		if progressBars > len(bar.rate) {
			bar.rate = strings.Repeat(bar.graph, progressBars)
		}
		bar.lastUpdate = currentTime
		// 使用回车符覆盖当前行，避免刷屏
		fmt.Printf("\r\033[K[%-30s]%3d%%  %s%5d/100", bar.rate, bar.percent, fmt.Sprintf("%s:", bar.taskUnit), bar.percent)
	}
}

// NewTableProgress 开始新表的进度显示，先输出换行再开始进度条
func (bar *Bar) NewTableProgress(tableName string) {
	// 先输出换行确保新表进度在新行开始
	fmt.Printf("\n%-40s", tableName)
}

//由于上面的打印没有打印换行符，因此，在进度全部结束之后（也就是跳出循环之外时），需要打印一个换行符，因此，封装了一个Finish函数，该函数纯粹的打印一个换行，表示进度条已经完成。
func (bar *Bar) Finish() {
	// 确保进度条显示100%完成
	if bar.cur < bar.total {
		bar.cur = bar.total
		bar.percent = 100
		// 补全进度条
		for len(bar.rate) < 30 {
			bar.rate += bar.graph
		}
		fmt.Printf("\r\033[K[%-极30s]%3d%%  %s%5d/100", bar.rate, bar.percent, fmt.Sprintf("%s:", bar.taskUnit), bar.percent)
	} else if bar.percent == 100 {
		// 如果进度已经是100%，确保进度条显示完整
		for len(bar.rate) < 30 {
			bar.rate += bar.graph
		}
		fmt.Printf("\r\033[K[%-30s]%3d%%  %s%5d/100", bar.rate, bar.percent, fmt.Sprintf("%s:", bar.taskUnit), bar.percent)
	}
	fmt.Println()
}
