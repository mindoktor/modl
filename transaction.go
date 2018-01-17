package modl

import (
	"database/sql"

	"mindoktor.io/sqlx"
	"context"
)

// Transaction represents a database transaction.
// Insert/Update/Delete/Get/Exec operations will be run in the context
// of that transaction.  Transactions should be terminated with
// a call to Commit() or Rollback()
type Transaction struct {
	dbmap *DbMap
	Tx    *sqlx.Tx
}

// Insert has the same behavior as DbMap.Insert(), but runs in a transaction.
func (t *Transaction) InsertContext(ctx context.Context, list ...interface{}) error {
	return insert(ctx, t.dbmap, t, list...)
}

// Update has the same behavior as DbMap.Update(), but runs in a transaction.
func (t *Transaction) UpdateContext(ctx context.Context, list ...interface{}) (int64, error) {
	return update(ctx, t.dbmap, t, list...)
}

// Delete has the same behavior as DbMap.Delete(), but runs in a transaction.
func (t *Transaction) DeleteContext(ctx context.Context, list ...interface{}) (int64, error) {
	return deletes(ctx, t.dbmap, t, list...)
}

// Get has the Same behavior as DbMap.Get(), but runs in a transaction.
func (t *Transaction) GetContext(ctx context.Context, dest interface{}, keys ...interface{}) error {
	return get(ctx, t.dbmap, t, dest, keys...)
}

// Select has the Same behavior as DbMap.Select(), but runs in a transaction.
func (t *Transaction) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return hookedselect(ctx, t.dbmap, t, dest, query, args...)
}

func (t *Transaction) SelectOneContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return hookedget(ctx, t.dbmap, t, dest, query, args...)
}

// Exec has the same behavior as DbMap.Exec(), but runs in a transaction.
func (t *Transaction) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	t.dbmap.trace(query, args)
	return t.Tx.Exec(query, args...)
}

// Commit commits the underlying database transaction.
func (t *Transaction) Commit() error {
	t.dbmap.trace("commit;")
	return t.Tx.Commit()
}

// Rollback rolls back the underlying database transaction.
func (t *Transaction) Rollback() error {
	t.dbmap.trace("rollback;")
	return t.Tx.Rollback()
}

func (t *Transaction) handle() handle {
	return &tracingHandle{h: t.Tx, d: t.dbmap}
}
