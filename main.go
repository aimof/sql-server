package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
)

var (
	ErrNotSupportDB   = errors.New("not support db type")
	ErrInvalidRequest = errors.New("invalid request")
)

func fatalError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

type Connectiion struct {
	conn map[string]*sqlx.DB
}

type Request struct {
	DBType string
	Method string
	Body   string
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
	// sqlite3: [connection, create, update, delete, insert, select]
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

func doExecSQL(db *sqlx.DB, sql string) error {
	_, err := db.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

func doQuerySQL(db *sqlx.DB, sql string) ([]map[string]interface{}, error) {
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

func doNewDBConn(dbType, host, user, password string) (*sqlx.DB, error) {
	var dns string
	switch dbType {
	//case "mysql":
	case "sqlite3":
		dns = ":memory:"
	}

	if dns == "" {
		return nil, ErrNotSupportDB
	}

	return sqlx.Open(dbType, dns)
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

		fmt.Print(strings.Split(string(buf), "\n"))
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
