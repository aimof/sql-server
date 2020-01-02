package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"

	_ "github.com/mattn/go-sqlite3"
)

var db *sqlx.DB

func init() {
	var err error
	db, err = sqlx.Open("sqlite3", ":memory:")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func TestDoExecSQL(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		sql := `
create table users(id int, name varchar(255))
`
		if err := doExecSQL(db, sql); err != nil {
			t.Fatal(errors.Wrap(err, "can't create table"))
		}
	})

	t.Run("insert", func(t *testing.T) {
		sql := `insert into users values (1, "gorilla")`

		if err := doExecSQL(db, sql); err != nil {
			t.Fatal(errors.Wrap(err, "can't insert data"))
		}
	})
}

func TestDoQuerySQL(t *testing.T) {
	sql := `
select * from users
`
	res, err := doQuerySQL(db, sql)
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
