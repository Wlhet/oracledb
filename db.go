package oracledb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type OracleClient struct {
	db   *sql.DB
	conf *OracleConfig
}

type Params map[string]interface{}

type WhereCase map[string]interface{}

type ModelInfo struct {
	FeildsName  []string
	FeildTypes  []string
	FeildTags   []string
	FieldValues []interface{}
}

type QueryModel interface {
	TableName() string
}

// 构造函数
func NewOracleClient(conf *OracleConfig) (*OracleClient, error) {
	if conf == nil {
		return nil, errors.New("config is nil")
	}
	if conf.driverName == "" {
		conf.driverName = "oracle"
	}
	if conf.userName == "" {
		return nil, errors.New("username is empty")
	}
	if conf.passWord == "" {
		return nil, errors.New("password is empty")
	}
	if conf.host == "" {
		return nil, errors.New("host is empty")
	}
	if conf.port == 0 {
		return nil, errors.New("port is empty")
	}
	if conf.dataBase == "" {
		return nil, errors.New("database is empty")
	}
	conf.EnableRecoverPanic()
	db, err := sql.Open(conf.driverName, conf.GetDsn())
	if err != nil {
		return nil, err
	}
	return &OracleClient{db: db, conf: conf}, nil
}

func (oc *OracleClient) GetConfig() *OracleConfig {
	return oc.conf
}

func (oc *OracleClient) GetDB() *sql.DB {
	return oc.db
}

func (o *OracleClient) SetConnMaxIdleTime(d time.Duration) {
	if o.db != nil {
		return
	}
	o.db.SetConnMaxIdleTime(d)
}
func (o *OracleClient) SetConnMaxLifetime(d time.Duration) {
	if o.db != nil {
		return
	}
	o.db.SetConnMaxLifetime(d)
}

func (o *OracleClient) SetMaxIdleConns(i int) {
	if o.db != nil {
		return
	}
	o.db.SetMaxIdleConns(1)
}

func (o *OracleClient) SetMaxOpenConns(i int) {
	if o.db != nil {
		return
	}
	o.db.SetMaxOpenConns(1)
}

func (o *OracleClient) Begin() (*sql.Tx, error) {
	return o.db.Begin()
}

func (o *OracleClient) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return o.db.BeginTx(ctx, opts)
}

func (o *OracleClient) Close() error {
	return o.db.Close()
}

func (oc *OracleClient) Ping() error {
	if oc.db == nil {
		return errors.New("db is nil")
	}
	oc.db.Begin()
	return oc.db.Ping()
}

func (oc *OracleClient) QueryWithDest(dest interface{}, sqlStr string, args ...interface{}) (err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	if strings.Contains(sqlStr, `"`) {
		return errors.New(`oralce shuld use >'<  not  >"< in sqlStr `)
	}
	valuesPtr := reflect.ValueOf(dest)
	if valuesPtr.Kind() != reflect.Ptr {
		return errors.New("dest type need ptr")
	}
	values := valuesPtr.Elem()
	slice := values.Type()
	if slice.Kind() != reflect.Slice {
		return errors.New("dest type need slice")
	}
	base := slice.Elem()
	if base.Kind() != reflect.Struct {
		return errors.New("dest type need struct")
	}
	rows, err := oc.db.Query(sqlStr, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	md, _ := getModelInfo(base)
	num := 0
	for rows.Next() {
		value := reflect.New(base).Elem()
		if err := rows.Scan(md.FieldValues...); err != nil {
			return err
		}
		for i := 0; i < base.NumField(); i++ {
			fType := strings.ToLower(base.Field(i).Type.Name())
			rv := reflect.ValueOf(md.FieldValues[i]).Elem()
			switch fType {
			case "int64":
				value.Field(i).SetInt(rv.FieldByName("Int64").Int())
			case "string":
				value.Field(i).SetString(rv.FieldByName("String").String())
			case "float64":
				value.Field(i).SetFloat((rv.FieldByName("Float64").Float()))
			case "time":
				value.Field(i).SetInt(rv.FieldByName("Int64").Int())
			case "bool":
				value.Field(i).SetBool(rv.FieldByName("Bool").Bool())
			}
		}
		values.Set(reflect.Append(values, value))
		num += 1
	}
	if num == 0 {
		return errors.New("no rows in set")
	}
	return
}

func (oc *OracleClient) QueryRowWithDest(dest interface{}, sqlStr string, args ...interface{}) (err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	if strings.Contains(sqlStr, `"`) {
		return errors.New(`oralce shuld use >'<  not  >"< in sqlStr `)
	}
	destPtr := reflect.ValueOf(dest)
	if destPtr.Kind() != reflect.Ptr {
		return errors.New("dest type need ptr")
	}
	destV := destPtr.Elem()
	destT := destV.Type()
	if destT.Kind() != reflect.Struct {
		return errors.New("dest type need struct")
	}
	md, _ := getModelInfo(destT)
	err = oc.db.QueryRow(sqlStr, args...).Scan(md.FieldValues...)
	if err != nil {
		return err
	}
	for i := 0; i < destT.NumField(); i++ {
		fType := md.FeildTypes[i]
		rv := reflect.ValueOf(md.FieldValues[i]).Elem()
		switch fType {
		case "int64":
			destV.Field(i).SetInt(rv.FieldByName("Int64").Int())
		case "string":
			destV.Field(i).SetString(rv.FieldByName("String").String())
		case "float64":
			destV.Field(i).SetFloat((rv.FieldByName("Float64").Float()))
		case "time":
			destV.Field(i).SetInt(rv.FieldByName("Int64").Int())
		case "bool":
			destV.Field(i).SetBool(rv.FieldByName("Bool").Bool())
		}
	}
	return
}

// 反射结构体查询数据保存到Struct
func (oc *OracleClient) QueryRowWithWhereCase(tname string, whereCase WhereCase, dest interface{}) (err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	destPtr := reflect.ValueOf(dest)
	if destPtr.Kind() != reflect.Ptr {
		return errors.New("dest type need ptr")
	}
	destV := destPtr.Elem()
	destT := destV.Type()
	if destT.Kind() != reflect.Struct {
		return errors.New("dest type need struct")
	}
	md, _ := getModelInfo(destT)
	sqlStr, vals, err := generateSqlStrQuery(tname, md.FeildTags, whereCase, true)
	if err != nil {
		return err
	}
	err = oc.db.QueryRow(sqlStr, vals...).Scan(md.FieldValues...)
	if err != nil {
		return err
	}
	for i := 0; i < destT.NumField(); i++ {
		fType := md.FeildTypes[i]
		rv := reflect.ValueOf(md.FieldValues[i]).Elem()
		switch fType {
		case "int64":
			destV.Field(i).SetInt(rv.FieldByName("Int64").Int())
		case "string":
			destV.Field(i).SetString(rv.FieldByName("String").String())
		case "float64":
			destV.Field(i).SetFloat((rv.FieldByName("Float64").Float()))
		case "time":
			destV.Field(i).SetInt(rv.FieldByName("Int64").Int())
		case "bool":
			destV.Field(i).SetBool(rv.FieldByName("Bool").Bool())
		}
	}
	return
}

// 反射结构体查询数据保存到Slice
func (oc *OracleClient) QueryWithWhereCase(tname string, whereCase WhereCase, dest interface{}) (err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	valuesPtr := reflect.ValueOf(dest)
	if valuesPtr.Kind() != reflect.Ptr {
		return errors.New("need ptr")
	}
	values := valuesPtr.Elem()
	slice := values.Type()
	if slice.Kind() != reflect.Slice {
		return errors.New("need slice")
	}
	base := slice.Elem()
	if base.Kind() != reflect.Struct {
		return errors.New("need struct")
	}
	md, _ := getModelInfo(base)
	sqlStr, vals, err := generateSqlStrQuery(tname, md.FeildTags, whereCase, false)

	if err != nil {
		return err
	}
	rows, err := oc.db.Query(sqlStr, vals...)
	if err != nil {
		return err
	}
	defer rows.Close()
	num := 0
	for rows.Next() {
		value := reflect.New(base).Elem()
		if err := rows.Scan(md.FieldValues...); err != nil {
			return err
		}
		for i := 0; i < base.NumField(); i++ {
			fType := strings.ToLower(base.Field(i).Type.Name())
			rv := reflect.ValueOf(md.FieldValues[i]).Elem()
			switch fType {
			case "int64":
				value.Field(i).SetInt(rv.FieldByName("Int64").Int())
			case "string":
				value.Field(i).SetString(rv.FieldByName("String").String())
			case "float64":
				value.Field(i).SetFloat((rv.FieldByName("Float64").Float()))
			case "time":
				value.Field(i).SetInt(rv.FieldByName("Int64").Int())
			case "bool":
				value.Field(i).SetBool(rv.FieldByName("Bool").Bool())
			}
		}
		values.Set(reflect.Append(values, value))
		num += 1
	}
	if num == 0 {
		return errors.New("no rows in set")
	}
	return nil
}

func (oc *OracleClient) Update(tname string, update Params, whereCols WhereCase) (err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	sqlStr, values, err := generateSqlStrUpdate(tname, update, whereCols)
	if err != nil {
		return err
	}
	_, err = oc.db.Exec(sqlStr, values...)
	if err != nil {
		return err
	}
	return nil
}

func (oc *OracleClient) Delete(tname string, whereCols WhereCase) (err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	sqlStr, values, err := generateSqlStrDelete(tname, whereCols)
	if err != nil {
		return err
	}
	_, err = oc.db.Exec(sqlStr, values...)
	if err != nil {
		return err
	}
	return nil
}

func (oc *OracleClient) Insert(tname string, params Params) (err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	sqlStr, values, err := generateSqlStrInsert(tname, params)
	if err != nil {
		return err
	}
	_, err = oc.db.Exec(sqlStr, values...)
	if err != nil {
		return err
	}
	return nil
}

func getModelInfo(dest reflect.Type) (*ModelInfo, error) {
	var fieldNames []string
	var fieldValues []interface{}
	var fieldTypes []string
	var fieldTags []string
	for i := 0; i < dest.NumField(); i++ {
		fType := strings.ToLower(dest.Field(i).Type.Name())
		fTag := dest.Field(i).Tag.Get("sqlm")
		fName := strings.ToLower(dest.Field(i).Name)
		fieldNames = append(fieldNames, fName)
		fieldTypes = append(fieldTypes, fType)
		if fTag == "" {
			fieldTags = append(fieldTags, fName)
		} else {
			fieldTags = append(fieldTags, fTag)
		}
		switch fType {
		case "int64":
			fieldValues = append(fieldValues, &sql.NullInt64{})
		case "string":
			fieldValues = append(fieldValues, &sql.NullString{})
		case "float64":
			fieldValues = append(fieldValues, &sql.NullFloat64{})
		case "time":
			fieldValues = append(fieldValues, &sql.NullInt64{})
		case "bool":
			fieldValues = append(fieldValues, &sql.NullBool{})
		}
	}
	return &ModelInfo{FeildsName: fieldNames, FeildTypes: fieldTypes, FieldValues: fieldValues, FeildTags: fieldTags}, nil
}

func generateSqlStrQuery(tname string, fieldNames []string, whereCase WhereCase, onlyOne bool) (string, []interface{}, error) {
	var keys []string
	var vals []interface{}
	for k, v := range whereCase {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	whereFormat := ""
	keysLen := len(keys)
	for index, v := range keys {
		kvs := strings.Split(v, "__")
		if len(kvs) != 2 {
			return "", nil, errors.New(`you shuld like this  sqlStrm.Params{"xxx__eq":""}`)
		}
		switch kvs[1] {
		case "eq":
			v = kvs[0] + " = "
		case "gt":
			v = kvs[0] + " > "
		case "lt":
			v = kvs[0] + " < "
		case "gte":
			v = kvs[0] + " >= "
		case "lte":
			v = kvs[0] + " <= "
		case "like":
			v = kvs[0] + " like "
		}
		if index != keysLen-1 {
			whereFormat = whereFormat + v + fmt.Sprintf(" :x%d AND ", index)
		} else {
			whereFormat = whereFormat + v + fmt.Sprintf(" :x%d ", index)
		}
	}
	var sqlStr string
	if len(whereCase) == 0 {
		sqlStr = fmt.Sprintf(`select %s from %s `, strings.Join(fieldNames, ", "), tname)
	} else {
		sqlStr = fmt.Sprintf(`select %s from %s where %s`, strings.Join(fieldNames, ", "), tname, whereFormat)
	}
	if onlyOne {
		sqlStr = sqlStr + "AND  rownum = 1"
	}
	return sqlStr, vals, nil
}

func generateSqlStrUpdate(tname string, update Params, whereCols WhereCase) (string, []interface{}, error) {
	if len(whereCols) == 0 || whereCols == nil {
		return "", nil, errors.New("must have lest one whereCase")
	}
	if len(update) == 0 || update == nil {
		return "", nil, errors.New("must have lest one updateCase")
	}
	updatesKeys := make([]string, 0)
	whereKeys := make([]string, 0)
	values := make([]interface{}, 0)
	for k, v := range update {
		updatesKeys = append(updatesKeys, k)
		values = append(values, v)
	}
	for k, v := range whereCols {
		whereKeys = append(whereKeys, k)
		values = append(values, v)
	}
	setsqlStr := ""
	updateLen := len(updatesKeys)
	for i, v := range updatesKeys {
		if i+1 == updateLen {
			setsqlStr = setsqlStr + v + fmt.Sprintf(" = :u%d ", i)
		} else {
			setsqlStr = setsqlStr + v + fmt.Sprintf(" = :u%d, ", i)
		}

	}
	wheresqlStr := ""
	whereLen := len(whereKeys)
	for i, v := range whereKeys {
		kvs := strings.Split(v, "__")
		if len(kvs) != 2 {
			return "", nil, errors.New(`you shuld like this  sqlStrm.WhereCase{"xxx__eq":""}`)
		}
		switch kvs[1] {
		case "eq":
			v = kvs[0] + " = "
		case "gt":
			v = kvs[0] + " > "
		case "lt":
			v = kvs[0] + " < "
		case "gte":
			v = kvs[0] + " >= "
		case "lte":
			v = kvs[0] + " <= "
		case "like":
			v = kvs[0] + " like "
		}
		if i+1 == whereLen {
			wheresqlStr = wheresqlStr + v + fmt.Sprintf("  :u%d ", i)
		} else {
			wheresqlStr = wheresqlStr + v + fmt.Sprintf("  :u%d, ", i)
		}

	}

	return fmt.Sprintf(`update %s set %s where %s`, tname, setsqlStr, wheresqlStr), values, nil
}

func generateSqlStrDelete(tname string, whereCols WhereCase) (string, []interface{}, error) {
	if len(whereCols) == 0 || whereCols == nil {
		return "", nil, errors.New("must have lest one whereCase")
	}
	whereKeys := make([]string, 0)
	values := make([]interface{}, 0)
	for k, v := range whereCols {
		whereKeys = append(whereKeys, k)
		values = append(values, v)
	}
	wheresqlStr := ""
	whereLen := len(whereKeys)
	for i, v := range whereKeys {
		kvs := strings.Split(v, "__")
		if len(kvs) != 2 {
			return "", nil, errors.New(`you shuld like this  sqlStrm.WhereCase{"xxx__eq":""}`)
		}
		switch kvs[1] {
		case "eq":
			v = kvs[0] + " = "
		case "gt":
			v = kvs[0] + " > "
		case "lt":
			v = kvs[0] + " < "
		case "gte":
			v = kvs[0] + " >= "
		case "lte":
			v = kvs[0] + " <= "
		case "like":
			v = kvs[0] + " like "
		}
		if i+1 == whereLen {
			wheresqlStr = wheresqlStr + v + fmt.Sprintf("  :u%d ", i)
		} else {
			wheresqlStr = wheresqlStr + v + fmt.Sprintf("  :u%d, ", i)
		}

	}

	return fmt.Sprintf(`delete from  %s  where %s`, tname, wheresqlStr), values, nil
}

func generateSqlStrInsert(tname string, params Params) (string, []interface{}, error) {
	if len(params) == 0 || params == nil {
		return "", nil, errors.New("must have lest one params")
	}
	paramsKeys := make([]string, 0)
	paramsValues := make([]interface{}, 0)

	for k, v := range params {
		paramsKeys = append(paramsKeys, k)
		paramsValues = append(paramsValues, v)

	}
	keysStr := ""
	valuesStr := ""
	whereLen := len(paramsKeys)
	for i, v := range paramsKeys {
		if i+1 == whereLen {
			keysStr = keysStr + v
			valuesStr = valuesStr + fmt.Sprintf(" :v%d ", i)
		} else {
			keysStr = keysStr + v + ", "
			valuesStr = valuesStr + fmt.Sprintf(" :v%d, ", i)
		}

	}

	return fmt.Sprintf(`insert into %s(%s) values(%s)`, tname, keysStr, valuesStr), paramsValues, nil
}
