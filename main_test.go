package main

import (
	"testing"

	"github.com/pkg/errors"
)

func TestNewDBConn(t *testing.T) {
	t.Run("new db connection", func(t *testing.T) {
		if err := doNewDBConn("sqlite3", ":memory:"); err != nil {
			t.Fatalf("can't connect db: %s", err)
		}
	})
}

func TestDoExecSQL(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		sql := `
create table users(id int, name varchar(255))
`
		if err := doExecSQL("sqlite3", sql); err != nil {
			t.Fatal(errors.Wrap(err, "can't create table"))
		}
	})

	t.Run("insert", func(t *testing.T) {
		sql := `insert into users values (1, "gorilla")`

		if err := doExecSQL("sqlite3", sql); err != nil {
			t.Fatal(errors.Wrap(err, "can't insert data"))
		}
	})
}

func TestDoQuerySQL(t *testing.T) {
	sql := `
select * from users
`
	res, err := doQuerySQL("sqlite3", sql)
	if err != nil {
		t.Fatal(errors.Wrap(err, "can't query from users table"))
	}

	id := res[0]["id"]
	if id != int64(1) {
		t.Fatalf("want: %d, got: %d", 1, id)
	}

	name := res[0]["name"]
	if name != "gorilla" {
		t.Fatalf("want: %s, got: %s", "gorilla", name)
	}
}

func TestParseRequest(t *testing.T) {

	t.Run("test_request", func(t *testing.T) {
		bytes := []byte(`sqlite3: connection
dns=:memory:
`)

		req, err := parseRequest(bytes)
		if err != nil {
			t.Fatalf("want nil, got error: %s", err)
		}

		dbtype := "sqlite3"
		if req.DBType != dbtype {
			t.Fatalf("want:%s, got:%s", dbtype, req.DBType)
		}

		method := "connection"
		if req.Method != method {
			t.Fatalf("want:%s, got:%s", method, req.Method)
		}

		body := `dns=:memory:`
		if req.Body != body {
			t.Fatalf("want:%s, got:%s", body, req.Body)
		}

	})

	t.Run("test_invalid_request", func(t *testing.T) {
		tests := []struct {
			input  []byte
			except error
		}{
			{input: []byte(`sqlite3`), except: ErrInvalidRequest},
			{input: []byte(`sqlite3\naaa`), except: ErrInvalidRequest},
			{input: []byte(`sqlite3: connection`), except: ErrInvalidRequest},
			{input: []byte("sqlite3: connection\ndns=:memory:"), except: nil},
		}

		for _, te := range tests {
			_, err := parseRequest(te.input)
			if err != te.except {
				t.Fatalf("want: %s, got:%s", te.except, err)
			}
		}
	})

}
