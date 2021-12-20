package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
// Global variables are forbidden

type DbDesc struct {
	tables map[string]TableDesc
}

type TableDesc struct {
	Name   string
	fields map[string]FieldDesc
}

type FieldDesc struct {
	IndexInTable int
	Name         string
	Type         string
	Nullable     bool
	IsPrimaryKey bool
}

func (field FieldDesc) getDefault() interface{} {
	if field.Nullable {
		return nil
	} else {
		if field.Type == "int" {
			return 0
		} else {
			return ""
		}
	}
}

func (tDesc TableDesc) getFieldsArray() []FieldDesc {
	result := make([]FieldDesc, 0, len(tDesc.fields))
	for _, value := range tDesc.fields {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].IndexInTable < result[j].IndexInTable
	})
	return result
}


//RespError represents an error of API
type RespError struct {
	Error      string
	HTTPStatus int
}

//PrepApiAnswer returns bytes for an answer with the error
func (rErr RespError) PrepApiAnswer() []byte {
	return []byte("{ \"error\":\"" + rErr.Error + "\"}")
}

//This function writes a RespError to a passed http.ResponseWriter
func (rErr RespError) serve(w http.ResponseWriter) {
	apiData := rErr.PrepApiAnswer()
	w.Header().Set("Content-Type", http.DetectContentType(apiData))
	w.WriteHeader(rErr.HTTPStatus)
	_, err := w.Write(apiData)
	if err != nil {
		log.Println(err.Error())
	}
}

type Tables struct {
	Tables []string `json:"tables"`
}

type RespTables struct {
	Response Tables `json:"response"`
}

func NewDbExplorer(db *sql.DB) (handler http.Handler, err error) {

	//Assume that a database doesn't change while this program works
	//so description of a DB can be cashed in this case
	desc, err := initExplorer(db)
	if err != nil {
		return nil, err
	}

	m := http.NewServeMux()
	handler = NewRouter(db, *desc)
	m.Handle("/", handler)

	return handler, nil
}

func initExplorer(db *sql.DB) (*DbDesc, error) {
	knownTableNames, err := getTables(db)
	if err != nil {
		return nil, err
	}
	result := DbDesc{make(map[string]TableDesc)}
	for _, tableName := range knownTableNames {
		fields, err := getFields(db, tableName)
		if err != nil {
			return nil, err
		}
		result.tables[tableName] = TableDesc{tableName, fields}

	}
	return &result, nil
}

func getFields(db *sql.DB, name string) (map[string]FieldDesc, error) {

	fRows, err := db.Query("SHOW COLUMNS FROM " + name + ";")
	if err != nil {
		return nil, err
	}
	defer func() {
		err = fRows.Close()
		if err != nil {
			log.Println("error while closing rows:", err)
		}
	}()
	cols, err := fRows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]interface{}, len(cols))
	for i, _ := range cols {
		vals[i] = new(sql.RawBytes)
	}

	fields := make(map[string]FieldDesc, len(vals))
	idx := -1
	for fRows.Next() {
		idx++
		err = fRows.Scan(vals...)
		if err != nil {
			return nil, err
		}
		fDesc := FieldDesc{}
		fDesc.IndexInTable = idx
		for i, col := range cols {
			switch col {
			case "Field":
				fDesc.Name = string(*reflect.ValueOf(vals[i]).Interface().(*sql.RawBytes))
			case "Type":
				fDesc.Type = string(*reflect.ValueOf(vals[i]).Interface().(*sql.RawBytes))
			case "Null":
				fDesc.Nullable = string(*reflect.ValueOf(vals[i]).Interface().(*sql.RawBytes)) == "YES"
			case "Key":
				fDesc.IsPrimaryKey = string(*reflect.ValueOf(vals[i]).Interface().(*sql.RawBytes)) == "PRI"
			}
		}
		fields[fDesc.Name] = fDesc
	}

	return fields, nil
}

//Router is a handler
type Router struct {
	desc DbDesc
	db   *sql.DB
}

//ServeHTTP handles the request by passing it to the real
//handler
func (l *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	switch r.Method {

	case http.MethodGet:
		serveGet(w, r, l)

	case http.MethodPost:
		servePost(w, r, l)

	case http.MethodPut:
		servePut(w, r, l)

	case http.MethodDelete:
		serveDelete(w, r, l)

	default:
		//we have to send http.StatusInternalServerError when any error occurs instead of http.StatusMethodNotAllowed
		RespError{HTTPStatus: http.StatusInternalServerError, Error: "Method not allowed"}.serve(w)

	}

	log.Printf("%s %s %v", r.Method, r.URL.Path, time.Since(start))
}

//serveGet serves for http.MethodPut requests
func servePut(w http.ResponseWriter, r *http.Request, l *Router) {
	log.Printf("servePut %s %s", r.Method, r.URL.Path)

	pathSegments := strings.Split(r.URL.Path, "/")
	if len(pathSegments) < 2 {
		RespError{HTTPStatus: http.StatusNotFound, Error: "unknown table"}.serve(w)
		return
	}
	searchTable := pathSegments[1]

	var ok bool
	var foundTable TableDesc
	if foundTable, ok = l.desc.tables[searchTable]; !ok {
		RespError{HTTPStatus: http.StatusNotFound, Error: "unknown table"}.serve(w)
		return
	}

	fmt.Println("foundTable", foundTable)

	//read
	decoder := json.NewDecoder(r.Body)
	requestedParams := make(map[string]interface{}, len(foundTable.fields))
	err := decoder.Decode(&requestedParams)
	if err != nil {
		RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
		return
	}
	defer func() {
		err = r.Body.Close()
		if err != nil {
			log.Println("error while closing Body:", err)
		}
	}()
	log.Println("requestedParams", requestedParams)
	fields := foundTable.getFieldsArray()
	sqlQuery := foundTable.prepInsertSqlQuery()
	log.Println("prepared sql query:", sqlQuery)

	result := make([]interface{}, len(fields))

	var idx = -1
	for _, field := range fields {
		idx++
		//assume that a Primary Key always has auto increment
		//so we don't use a got value for this field
		if field.IsPrimaryKey {
			result[idx] = field.getDefault()
			continue
		}
		if found, ok := requestedParams[field.Name]; !ok || found == nil {
			result[idx] = field.getDefault()
		} else {
			result[idx] = found
		}
	}

	res, err := l.db.Exec(sqlQuery, result...)
	if err != nil {
		RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
		log.Println("db.Exec with err:", err, " passed values:", result)
		return
	}

	id, err := res.LastInsertId()
	if err != nil {
		log.Println("LastInsertId err:", err)
		RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
		return
	}
	serveAnswer(w, map[string]interface{}{"response": map[string]interface{}{"id": id}})
}

func (tDesc TableDesc) prepInsertSqlQuery() string {
	fields := tDesc.getFieldsArray()
	fieldsNumber := len(fields)
	values := make([]string, fieldsNumber)
	placeholders := make([]string, fieldsNumber)
	var index = -1
	for /*key*/ _, field := range fields {
		index++
		values[index] = field.Name
		placeholders[index] = "?"
	}
	return fmt.Sprintf("insert into %s (%s) values (%s)", tDesc.Name, strings.Join(values, ", "), strings.Join(placeholders, ", "))
}

//serveGet serves for http.MethodDelete requests
func serveDelete(w http.ResponseWriter, r *http.Request, l *Router) {
	log.Printf("serveDelete %s %s", r.Method, r.URL.Path)
}

//serveGet serves for http.MethodPost requests
func servePost(w http.ResponseWriter, r *http.Request, l *Router) {
	log.Printf("servePost %s %s", r.Method, r.URL.Path)
}

//serveGet serves for http.MethodGet requests
func serveGet(w http.ResponseWriter, r *http.Request, l *Router) {
	log.Printf("serveGet %s %s", r.Method, r.URL.Path)

	if r.URL.Path == "/" {
		serveListTables(w, l.desc)
		return
	}

	pathSegments := strings.Split(r.URL.Path[1:], "/")
	log.Printf("pathSegments %d %s", len(pathSegments), pathSegments)
	switch len(pathSegments) {
	case 1:
		serveListRows(l.db, w, l.desc, pathSegments[0], r.URL.Query())

	case 2:
		serveRowById(l.db, w, l.desc, pathSegments[0], pathSegments[1])

	default:
		RespError{HTTPStatus: http.StatusNotFound, Error: "Not Found"}.serve(w)

	}

}

func serveRowById(db *sql.DB, w http.ResponseWriter, desc DbDesc, tableName string, id string) {
	if foundTable, ok := desc.tables[tableName]; ok {
		sqlQ := fmt.Sprintf("SELECT * FROM %s WHERE id = ?", tableName)
		res, err := db.Query(sqlQ, id)
		if err != nil {
			RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
			log.Println(err)
			return
		}
		defer func() {
			err = res.Close()
			if err != nil {
				log.Println("error while closing rows:", err)
			}
		}()

		cols, err := res.Columns()
		if err != nil {
			RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
			log.Println(err)
			return
		}
		vals := make([]interface{}, len(cols))
		for i, _ := range cols {
			vals[i] = new(sql.RawBytes)
		}

		rows := make([]interface{}, 0)
		for res.Next() {
			err = res.Scan(vals...)
			if err != nil {
				RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
				log.Println(err)
				return
			}
			row := make(map[string]interface{})

			for i, col := range cols {
				var val = *reflect.ValueOf(vals[i]).Interface().(*sql.RawBytes)
				if val != nil {
					var field FieldDesc
					if descField, ok := foundTable.fields[col]; ok {
						field = descField
					}
					if field.Type == "int" {
						r, _ := strconv.Atoi(string(val))
						row[col] = r
					} else {
						row[col] = string(val)
					}

				} else {

					row[col] = nil
				}
			}

			rows = append(rows, row)
		}
		if len(rows) == 1 {
			serveAnswer(w, map[string]interface{}{"response": map[string]interface{}{"record": rows[0]}})
		} else {
			RespError{HTTPStatus: http.StatusNotFound, Error: "record not found"}.serve(w)
		}

	} else {
		RespError{HTTPStatus: http.StatusNotFound, Error: "unknown table"}.serve(w)
	}
}

func serveListRows(db *sql.DB, w http.ResponseWriter, desc DbDesc, tableName string, query url.Values) {
	if found, ok := desc.tables[tableName]; ok {
		log.Println("found description:", found)
		limitStr := getIntValueAsStringFromQuery(query, "limit", "5")

		offsetStr := getIntValueAsStringFromQuery(query, "offset", "0")

		res2, err := db.Query("select * from " + tableName + " limit " + limitStr + " offset " + offsetStr)
		if err != nil {
			RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
			log.Println(err)
			return
		}

		defer func() {
			err = res2.Close()
			if err != nil {
				log.Println("error while closing rows:", err)
			}
		}()

		cols, err := res2.Columns()
		if err != nil {
			RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
			log.Println(err)
			return
		}
		vals := make([]interface{}, len(cols))
		for i, _ := range cols {
			vals[i] = new(sql.RawBytes)
		}

		rows := make([]interface{}, 0)
		for res2.Next() {
			err = res2.Scan(vals...)
			if err != nil {
				RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
				log.Println(err)
				return
			}
			row := make(map[string]interface{})

			for i, col := range cols {
				var val = *reflect.ValueOf(vals[i]).Interface().(*sql.RawBytes)
				if val != nil {
					var field FieldDesc
					if descField, ok := found.fields[col]; ok {
						field = descField
					}
					if field.Type == "int" {
						r, _ := strconv.Atoi(string(val))
						row[col] = r
					} else {
						row[col] = string(val)
					}

				} else {

					row[col] = nil
				}
			}

			rows = append(rows, row)
		}

		serveAnswer(w, map[string]interface{}{"response": map[string]interface{}{"records": rows}})
	} else {
		RespError{HTTPStatus: http.StatusNotFound, Error: "unknown table"}.serve(w)
	}
}

func getIntValueAsStringFromQuery(query url.Values, key string, defaultValue string) string {
	result := query.Get(key)
	if result == "" {
		result = defaultValue
	} else { //to prevent injections
		_, err := strconv.Atoi(result)
		if err != nil {
			result = defaultValue
		}
	}
	return result
}

func serveListTables(w http.ResponseWriter, desc DbDesc) {
	resp := RespTables{}
	for key, _ := range desc.tables {
		resp.Response.Tables = append(resp.Response.Tables, key)
	}
	sort.Slice(resp.Response.Tables, func(i, j int) bool {
		return resp.Response.Tables[i] < resp.Response.Tables[j]
	})
	serveAnswer(w, resp)
}

func serveAnswer(w http.ResponseWriter, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("can't json.Marshal by err [%s] with:\n %+v\n", err.Error(), v)
		RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}.serve(w)
		return
	}
	w.Header().Set("Content-Type", http.DetectContentType(data))
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(data)
	if err != nil {
		log.Println("can't serve:" + err.Error())
	}
}

//NewRouter constructs a new Router middleware handler
func NewRouter(db *sql.DB, desc DbDesc) *Router {
	return &Router{desc, db}
}

//getTables returns a list of tables or error
func getTables(db *sql.DB) (tables []string, err error) {
	if db == nil {
		return nil, errors.New("*sql.Db is <nil>")
	}
	var res *sql.Rows
	res, err = db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer func() {
		err = res.Close()
		if err != nil {
			log.Println("error while closing rows:", err)
		}
	}()
	var table string

	for res.Next() {
		err = res.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return
}
