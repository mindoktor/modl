package modl

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"context"
)

// a cursor is either an sqlx.Db or an sqlx.Tx
type handle interface {
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	Exec(query string, args ...interface{}) (sql.Result, error)

	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// an implmentation of handle which traces using dbmap
type tracingHandle struct {
	d *DbMap
	h handle
}

func (t *tracingHandle) Select(dest interface{}, query string, args ...interface{}) error {
	t.d.trace(query, args...)
	return t.h.Select(dest, query, args...)
}

func (t *tracingHandle) Get(dest interface{}, query string, args ...interface{}) error {
	t.d.trace(query, args...)
	return t.h.Get(dest, query, args...)
}

func (t *tracingHandle) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	t.d.trace(query, args...)
	return t.h.Queryx(query, args...)
}

func (t *tracingHandle) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	t.d.trace(query, args...)
	return t.h.QueryRowx(query, args...)
}

func (t *tracingHandle) Exec(query string, args ...interface{}) (sql.Result, error) {
	t.d.trace(query, args...)
	return t.h.Exec(query, args...)
}

func (t *tracingHandle) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	t.d.trace(query, args...)
	return t.h.SelectContext(ctx, dest, query, args...)
}

func (t *tracingHandle) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	t.d.trace(query, args...)
	return t.h.GetContext(ctx, dest, query, args...)
}

func (t *tracingHandle) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	t.d.trace(query, args...)
	return t.h.QueryxContext(ctx, query, args...)
}

func (t *tracingHandle) QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row {
	t.d.trace(query, args...)
	return t.h.QueryRowxContext(ctx, query, args...)
}

func (t *tracingHandle) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	t.d.trace(query, args...)
	return t.h.ExecContext(ctx, query, args...)
}
