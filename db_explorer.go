package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
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
		//we have to send http.StatusInternalServerError when any error occurs
		http.Error(w, "Method not allowed", http.StatusInternalServerError /*http.StatusMethodNotAllowed*/)
		return
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

	resp := RespTables{}
	for key, _ := range l.desc.tables {
		resp.Response.Tables = append(resp.Response.Tables, key)
	}
	b, _ := json.Marshal(resp)
	w.Write(b)
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
