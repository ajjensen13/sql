// Package sql provides opinionated interfaces around the database/sql implementations. In general, they are they
// same except:  1) they accepts context.Context parameters without using the *Context suffix. 2) types are
// interfaces so they can be easily mocked in tests. 3) Scanner represents a row or rows, rather than a column.
package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

var (
	_ DB   = db{}
	_ Stmt = stmt{}
	_ Tx   = tx{}
)

// The following are interface wrappers around concrete types in database/sql.
type (
	DB interface {
		Ping(ctx context.Context) error
		Prepare(ctx context.Context, query string) (Stmt, error)
		BeginTx(ctx context.Context, opts *TxOptions) (Tx, error)
		Exec(ctx context.Context, query string, args ...interface{}) (Result, error)
		Query(ctx context.Context, query string, args ...interface{}) (*Rows, error)
		QueryRow(ctx context.Context, query string, args ...interface{}) *Row
		Driver() driver.Driver
		StdDB() *sql.DB
	}
	Conn interface {
		Ping(ctx context.Context) error
		Prepare(ctx context.Context, query string) (Stmt, error)
		BeginTx(ctx context.Context, opts *TxOptions) (Tx, error)
		Exec(ctx context.Context, query string, args ...interface{}) (Result, error)
		Query(ctx context.Context, query string, args ...interface{}) (*Rows, error)
		QueryRow(ctx context.Context, query string, args ...interface{}) *Row
		Close() error
		StdConn() *sql.Conn
	}
	Stmt interface {
		Exec(ctx context.Context, args ...interface{}) (Result, error)
		Query(ctx context.Context, args ...interface{}) (*Rows, error)
		QueryRow(ctx context.Context, args ...interface{}) *Row
		Close() error
		StdStmt() *sql.Stmt
	}
	Tx interface {
		Commit() error
		Rollback() error
		Prepare(ctx context.Context, query string) (Stmt, error)
		Exec(ctx context.Context, query string, args ...interface{}) (Result, error)
		Query(ctx context.Context, query string, args ...interface{}) (*Rows, error)
		QueryRow(ctx context.Context, query string, args ...interface{}) *Row
		StdTx() *sql.Tx
	}
)

// The following types are simple aliases that are exported to make it easier to consume this package.
// These aliases allow users to use this package without also importing database/sql, thus causing a
// package name collision.
type (
	TxOptions      = sql.TxOptions
	Row            = sql.Row
	Rows           = sql.Rows
	IsolationLevel = sql.IsolationLevel
	Result         = sql.Result
	NamedArg       = sql.NamedArg
	NullBool       = sql.NullBool
	NullFloat64    = sql.NullFloat64
	NullInt32      = sql.NullInt32
	NullInt64      = sql.NullInt64
	NullString     = sql.NullString
	NullTime       = sql.NullTime
	Out            = sql.Out
	RawBytes       = sql.RawBytes
	DBStats        = sql.DBStats
)

// The following types are implementations of the interface wrappers exported by this package
type (
	db struct {
		*sql.DB
	}
	stmt struct {
		*sql.Stmt
	}
	tx struct {
		*sql.Tx
	}
)

func (d db) StdDB() *sql.DB {
	return d.DB
}

func Open(driverName, dataSourceName string) (DB, error) {
	result, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return db{result}, nil
}

func OpenDB(c driver.Connector) (DB, error) {
	return db{sql.OpenDB(c)}, nil
}

func (d db) Ping(ctx context.Context) error {
	return d.DB.PingContext(ctx)
}

func (d db) Prepare(ctx context.Context, query string) (Stmt, error) {
	result, err := d.DB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt{result}, nil
}

func (d db) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return d.ExecContext(ctx, query, args...)
}

func (d db) Query(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	return d.QueryContext(ctx, query, args...)
}

func (d db) QueryRow(ctx context.Context, query string, args ...interface{}) *Row {
	return d.QueryRowContext(ctx, query, args...)
}

func (d db) BeginTx(ctx context.Context, opts *TxOptions) (Tx, error) {
	result, err := d.DB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return tx{result}, nil
}

func (d stmt) StdStmt() *sql.Stmt {
	return d.Stmt
}

func (d stmt) Exec(ctx context.Context, args ...interface{}) (Result, error) {
	return d.Stmt.ExecContext(ctx, args...)
}

func (d stmt) Query(ctx context.Context, args ...interface{}) (*Rows, error) {
	return d.Stmt.QueryContext(ctx, args...)
}

func (d stmt) QueryRow(ctx context.Context, args ...interface{}) *Row {
	return d.Stmt.QueryRowContext(ctx, args...)
}

func (t tx) StdTx() *sql.Tx {
	return t.Tx
}

func (t tx) Prepare(ctx context.Context, query string) (Stmt, error) {
	result, err := t.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt{result}, nil
}

func (t tx) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return t.ExecContext(ctx, query, args...)
}

func (t tx) Query(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	return t.QueryContext(ctx, query, args...)
}

func (t tx) QueryRow(ctx context.Context, query string, args ...interface{}) *Row {
	return t.QueryRowContext(ctx, query, args...)
}

// Scanner abstracts sql.Rows and sql.Row. Note: this is DIFFERENT
// than the sql.Scanner interface.
type Scanner interface {
	Scan(...interface{}) error
}

// Named exposes database/sql.Named(string, interface{})
func Named(name string, value interface{}) NamedArg {
	return sql.Named(name, value)
}
