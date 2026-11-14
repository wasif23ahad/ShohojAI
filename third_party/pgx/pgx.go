// Package pgx provides PostgreSQL interfaces, row scanners, and transaction types.
package pgx

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrNoRows is returned when QueryRow returns no rows.
var ErrNoRows = errors.New("no rows in result set")

// Row represents a single database row returned by QueryRow.
type Row interface {
	Scan(dest ...any) error
}

// Rows represents a cursor over multiple database rows returned by Query.
type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(dest ...any) error
}

// TxIsoLevel represents a PostgreSQL transaction isolation level.
type TxIsoLevel string

const (
	Serializable   TxIsoLevel = "serializable"
	RepeatableRead TxIsoLevel = "repeatable read"
	ReadCommitted  TxIsoLevel = "read committed"
	ReadUncommitted TxIsoLevel = "read uncommitted"
)

// TxAccessMode represents the transaction access mode.
type TxAccessMode string

const (
	ReadWrite TxAccessMode = "read write"
	ReadOnly  TxAccessMode = "read only"
)

// TxOptions configures a PostgreSQL transaction.
type TxOptions struct {
	IsoLevel   TxIsoLevel
	AccessMode TxAccessMode
}

// Tx represents an active PostgreSQL database transaction.
type Tx interface {
	Begin(ctx context.Context) (Tx, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
}
