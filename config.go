package oracledb

import (
	"github.com/wlhet/orago"
)

type OracleConfig struct {
	driverName   string //驱动
	userName     string //用户名
	passWord     string //密码
	host         string //主机
	dataBase     string //数据库
	traceFile    string //日志文件名
	port         int    //端口
	recoverPanic bool   //捕获恐慌
}

// 数据库请求日志
func (oc *OracleConfig) GetTraceFile() string {
	return oc.traceFile
}
func (oc *OracleConfig) SetTraceFile(value string) {
	oc.traceFile = value
}

// 数据库用户名
func (oc *OracleConfig) GetUserName() string {
	return oc.userName
}
func (oc *OracleConfig) SetUserName(value string) {
	oc.userName = value
}

// 数据库密码
func (oc *OracleConfig) GetPassWord() string {
	return oc.passWord
}
func (oc *OracleConfig) SetPassWord(value string) {
	oc.passWord = value
}

// 数据库驱动名
func (oc *OracleConfig) GetDriverName() string {
	return oc.driverName
}
func (oc *OracleConfig) SetDriverName(value string) {
	oc.driverName = value
}

// 数据库主机地址
func (oc *OracleConfig) GetHost() string {
	return oc.host
}
func (oc *OracleConfig) SetHost(value string) {
	oc.host = value
}

// 数据库主机端口
func (oc *OracleConfig) GetPort() int {
	return oc.port
}
func (oc *OracleConfig) SetPort(value int) {
	oc.port = value
}

func (oc *OracleConfig) GetDataBase() string {
	return oc.dataBase
}

func (oc *OracleConfig) SetDataBase(value string) {
	oc.dataBase = value
}

func (oc *OracleConfig) GetDsn() string {
	if oc.traceFile != "" {
		return orago.BuildUrl(oc.host, oc.port, oc.dataBase, oc.userName, oc.passWord, map[string]string{"TRACE FILE": oc.traceFile})
	} else {
		return orago.BuildUrl(oc.host, oc.port, oc.dataBase, oc.userName, oc.passWord, nil)
	}

}

// 捕获恐慌,防止驱动不稳定导致程序退出
func (oc *OracleConfig) EnableRecoverPanic() {
	oc.recoverPanic = true
}
func (oc *OracleConfig) DisableRecoverPanic() {
	oc.recoverPanic = false
}
