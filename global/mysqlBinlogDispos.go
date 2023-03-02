package global

import "strings"

type binlogInfoStruct struct{
	evetRotate   RotateEventStruct
	formatdesEvent  FormatDescriptionEventStruct
	PreviousGTIDsEvent  PreviousGTIDsEventStruct
	GtidEvent     GtidEventStruct
	QueryEvent     QueryEventStruct
	TableMapEvent  TableMapEventStruct
	WriteRowsEventV2   WriteRowsEventV2Struct
	UpdateRowsEventV2  UpdateRowsEventV2Struct
	DeleteRowsEventV2   DeleteRowsEventV2Struct
	XidEvent       XidEventStruct
}

type RotateEventStruct struct {
	Datetime  string
	LogPosition string
	EventSize   string
	Position    string
	NextLogName  string
}
type FormatDescriptionEventStruct struct {
	Datetime  string
	LogPosition string
	EventSize   string
	Version    string
	ServerVersion  string
	ChecksumAlgorithm string
}
type PreviousGTIDsEventStruct struct {
	Datetime  string
	LogPosition string
	EventSize   string
	GtidEvent   string
}
type GtidEventStruct struct{
	Datetime  string
	LogPosition string
	EventSize   string
	GtidNext    string
	LastCommitted string
	sequenceNumber string
	transactionLength string
}
type QueryEventStruct struct{
	Datetime  string
	LogPosition string
	EventSize   string
	Executiontime   string
	ErrorCode    string
	Schema       string
	Query         string
}
type TableMapEventStruct struct {
	Datetime  string
	LogPosition string
	EventSize   string
	TableID     string
	TableIDSize string
	Schema      string
	Table       string
}
type WriteRowsEventV2Struct struct {
	Datetime  string
	LogPosition string
	EventSize   string
	TableID     string
	ColumnCount string
	Values     string
}
type UpdateRowsEventV2Struct struct {
	Datetime  string
	LogPosition string
	EventSize   string
	TableID     string
	ColumnCount string
	Values     string
}
type DeleteRowsEventV2Struct struct {
	Datetime  string
	LogPosition string
	EventSize   string
	TableID     string
	ColumnCount string
	Values     string
}

type XidEventStruct struct {
	Datetime string
	LogPosition string
	EventSize string
	Xid       string
}


func (binlog *binlogInfoStruct) EventFilter(tmpb []string) *binlogInfoStruct{
	var binlogInfo = &binlogInfoStruct{}
	dmlStatus := true
	for i := range tmpb {
		if strings.HasPrefix(tmpb[i],"=== RotateEvent ==="){
		}
		if strings.HasPrefix(tmpb[i],"=== FormatDescriptionEvent ==="){
		}
		if strings.HasPrefix(tmpb[i],"=== PreviousGTIDsEvent ==="){

		}

		if strings.HasPrefix(tmpb[i],"=== GTIDEvent ==="){
			if strings.HasPrefix(tmpb[i], "GTID_NEXT") {
				binlogInfo.GtidEvent.GtidNext = strings.Split(tmpb[i], ": ")[1]
			}
		}
		if strings.HasPrefix(tmpb[i],"=== QueryEvent ==="){
			if strings.HasPrefix(tmpb[i], "Query") {
				if strings.Split(tmpb[i], ": ")[1] == "BEGIN"{
					dmlStatus = true
				}else{
					dmlStatus = false
					binlogInfo.GtidEvent.GtidNext = strings.Split(tmpb[i], ": ")[1]
				}
			}
		}
		if dmlStatus && strings.HasPrefix(tmpb[i],"=== TableMapEvent ==="){

		}
		if dmlStatus && strings.HasPrefix(tmpb[i],"=== WriteRowsEventV2 ==="){

		}
		if dmlStatus && strings.HasPrefix(tmpb[i],"=== UpdateRowsEventV2 ==="){

		}
		if dmlStatus && strings.HasPrefix(tmpb[i],"=== DeleteRowsEventV2 ==="){

		}

		//if strings.HasPrefix(tmpb[i], "Log position") {
		//	binl.binlogPos = strings.Split(tmpb[i], ": ")[1]
		//}
		//if strings.HasPrefix(tmpb[i], "Next log name") {
		//	binl.binlogFile = strings.Split(tmpb[i], ": ")[1]
		//}
		//if strings.HasPrefix(tmpb[i], "Date") {
		//	binl.dateTime = strings.Split(tmpb[i], ": ")[1]
		//}
		//if strings.HasPrefix(tmpb[i], "Version") {
		//	binl.version = strings.Split(tmpb[i], ": ")[1]
		//}
		//if strings.HasPrefix(tmpb[i], "Server version") {
		//	binl.serverVersion = strings.Split(tmpb[i], ": ")[1]
		//}
		//if strings.HasPrefix(tmpb[i], "Checksum algorithm") {
		//	binl.checksumAlgorithm = strings.Split(tmpb[i], ": ")[1]
		//}

	}
	return binlogInfo
}