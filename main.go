package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/jmoiron/sqlx"
)

var (
	ErrNotSupportDB = errors.New("not support db type")
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
	SQL    string
}

func parseRequest(bytes []byte) Request {
	var req Request
	return req
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

		fmt.Println(string(buf))
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
