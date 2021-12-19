package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"
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
	Name string
}

var errNotFound = RespError{HTTPStatus: http.StatusNotFound, Error: "Not Found"}
var errUnknownTable = RespError{HTTPStatus: http.StatusNotFound, Error: "unknown table"}
var errInternalError = RespError{HTTPStatus: http.StatusInternalServerError, Error: "Internal Server Error"}

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
		//TODO gather information about tables
		result.tables[tableName] = TableDesc{tableName, nil /*stub*/}
	}
	return &result, nil
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

	pathSegments := strings.Split(r.URL.Path[:0], "/")
	log.Printf("pathSegments %d %s", len(pathSegments), pathSegments)
	switch len(pathSegments) {
	case 1:
		serveListFields(w, l.desc, pathSegments[0])

	case 2:
		//TODO the proper answer
		errInternalError.serve(w)
	default:
		errNotFound.serve(w)

	}

}

func serveListFields(w http.ResponseWriter, desc DbDesc, tableName string) {
	if found, ok := desc.tables[tableName]; ok {
		log.Println("found description:", found)
		//TODO a good answer, errInternalError is a stub
		errInternalError.serve(w)
	} else {
		errUnknownTable.serve(w)
	}
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
		errInternalError.serve(w)
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
