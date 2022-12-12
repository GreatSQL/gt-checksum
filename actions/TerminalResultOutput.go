package actions

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/gosuri/uitable"
	"greatdbCheck/inputArg"
	"strings"
)

type Pod struct {
	Schema, Table, IndexCol, CheckMod, Rows, Differences, CheckObject, Datafix, FuncName, Definer, ProcName, TriggerName string
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
		table.AddRow("Schema", "Table ", "CheckObject ", "IndexCol ", "Differences", "Datafix")
		for _, pod := range measuredDataPods {
			table.AddRow(color.RedString(pod.Schema), color.WhiteString(pod.Table), color.RedString(pod.CheckObject), color.YellowString(pod.IndexCol), color.GreenString(pod.Differences), color.YellowString(pod.Datafix))
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
