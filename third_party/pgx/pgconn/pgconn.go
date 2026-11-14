// Package pgconn defines command tags and low-level connection types for pgx.
package pgconn

import "fmt"

// CommandTag represents the result tag returned by a SQL command (e.g. "INSERT 0 1").
type CommandTag struct {
	tag          string
	rowsAffected int64
}

// NewCommandTag constructs a CommandTag with the given tag string and rows affected count.
func NewCommandTag(tag string, rowsAffected int64) CommandTag {
	return CommandTag{tag: tag, rowsAffected: rowsAffected}
}

// RowsAffected returns the number of rows affected by the command.
func (ct CommandTag) RowsAffected() int64 {
	return ct.rowsAffected
}

// String returns the command tag text.
func (ct CommandTag) String() string {
	if ct.tag != "" {
		return ct.tag
	}
	return fmt.Sprintf("AFFECTED %d", ct.rowsAffected)
}
