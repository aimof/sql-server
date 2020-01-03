package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var (
	ErrNotSupportDB     = errors.New("not support db type")
	ErrNotSupportMethod = errors.New("not support method")
	ErrInvalidRequest   = errors.New("invalid request")
	ErrNoDBConnection   = errors.New("no db connection")
	ErrInvalidDSN       = errors.New("invalid dsn")
)

const (
	StatusSuccess = "success"
	StatusError   = "error"
)

var connectionPool = Connection{
	conn: make(map[string]*sqlx.DB),
}

func fatalError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func printError(err error) {
	fmt.Fprintln(os.Stderr, err)
}

type Connection struct {
	conn map[string]*sqlx.DB
}

type Request struct {
	DBType string
	Method string
	Body   string
}

type Response struct {
	Status string `json:"status"`
	Method string `json:"method"`
	Body   string `json:"body"`
}

func parseRequest(bytes []byte) (*Request, error) {
	b := strings.Split(string(bytes), "\n")
	if len(b) < 2 {
		return nil, ErrInvalidRequest
	}

	// header format
	// DBTYPE: METHOD
	//
	// e.g
	// sqlite3: [connection, exec]
	h := strings.Split(b[0], ":")
	if len(h) < 2 {
		return nil, ErrInvalidRequest
	}
	dbtype := strings.TrimSpace(h[0])
	method := strings.TrimSpace(h[1])
	body := strings.TrimSpace(strings.Join(b[1:], "\n"))

	req := &Request{
		DBType: dbtype,
		Method: method,
		Body:   body,
	}

	return req, nil
}

func parseDSN(body string) (string, error) {
	b := strings.Split(body, "=")
	if len(b) < 2 {
		return "", ErrInvalidDSN
	}

	return b[1], nil
}

var validDBType = map[string]struct{}{
	"sqlite3": struct{}{},
	"mysql":   struct{}{},
}

var validMethod = map[string]struct{}{
	"connection": struct{}{},
	"query":      struct{}{},
	"exec":       struct{}{},
}

func validateRequest(req *Request) error {
	if _, ok := validDBType[req.DBType]; !ok {
		return ErrNotSupportDB
	}

	if _, ok := validMethod[req.Method]; !ok {
		return ErrNotSupportMethod
	}

	return nil
}

func getDBConn(dbtype string) (*sqlx.DB, error) {
	db, ok := connectionPool.conn[dbtype]
	if !ok {
		return nil, ErrNoDBConnection
	}
	return db, nil
}

func doExecSQL(dbtype, sql string) error {
	db, err := getDBConn(dbtype)
	if err != nil {
		return err
	}

	_, err = db.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func doQuerySQL(dbtype, sql string) ([]map[string]interface{}, error) {
	db, err := getDBConn(dbtype)
	if err != nil {
		return nil, err
	}

	rows, err := db.Queryx(sql)
	if err != nil {
		return nil, err
	}

	result := []map[string]interface{}{}

	for rows.Next() {
		row := make(map[string]interface{})
		if err := rows.MapScan(row); err != nil {
			return nil, err
		}
		result = append(result, row)
	}

	return result, nil
}

func doNewDBConn(dbtype, dns string) error {
	db, err := sqlx.Open(dbtype, dns)
	if err != nil {
		return err
	}

	if _, ok := connectionPool.conn[dbtype]; ok {
		return nil
	}
	connectionPool.conn[dbtype] = db
	return nil
}

func writeToConn(conn net.Conn, msg string) {
	conn.Write([]byte(msg))
}

func writeResponse(conn net.Conn, status, method, body string) {
	res := Response{
		Status: status,
		Method: method,
		Body:   body,
	}

	b, err := json.Marshal(res)
	if err != nil {
		conn.Write([]byte(err.Error()))
		return
	}
	conn.Write(b)
}

func recive(conn net.Conn) {
	for {
		buf := make([]byte, 1024)
		if _, err := conn.Read(buf); err != nil {
			if err != io.EOF {
				fatalError(err)
			} else {
				fmt.Println("disconnect")
				break
			}
		}

		req, err := parseRequest(buf)

		if err != nil {
			writeResponse(conn, StatusError, "unknown", err.Error())
			continue
		}

		if err := validateRequest(req); err != nil {
			writeResponse(conn, StatusError, req.Method, err.Error())
			continue
		}

		if req.Method == "connection" {
			dsn, err := parseDSN(req.Body)
			if err != nil {
				writeResponse(conn, StatusError, req.Method, err.Error())
				continue
			}

			if err := doNewDBConn(req.DBType, dsn); err != nil {
				writeResponse(conn, StatusError, req.Method, err.Error())
				continue
			}

			writeResponse(conn, StatusSuccess, req.Method, req.DBType)
		} else if req.Method == "exec" {
			if err := doExecSQL(req.DBType, req.Body); err != nil {
				writeResponse(conn, StatusError, req.Method, err.Error())
				continue
			}

			writeResponse(conn, StatusSuccess, req.Method, "execute sql success")
		} else if req.Method == "query" {
			result, err := doQuerySQL(req.DBType, req.Body)

			if err != nil {
				writeResponse(conn, StatusError, req.Method, err.Error())
				continue
			}

			res, err := json.Marshal(result)
			if err != nil {
				conn.Write([]byte(err.Error()))
				continue
			}

			writeResponse(conn, StatusSuccess, req.Method, string(res))
		}
	}
}

func main() {
	listen, err := net.Listen("tcp", ":9999")
	if err != nil {
		fatalError(err)
	}

	fmt.Println("start sql server")

	for {
		conn, err := listen.Accept()
		if err != nil {
			fatalError(err)
		}

		recive(conn)
	}
}
