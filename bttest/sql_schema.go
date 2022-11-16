package bttest

import (
	"context"

	"github.com/jmoiron/sqlx"
)

func CreateTables(ctx context.Context, db *sqlx.DB) error {
	query := `CREATE TABLE IF NOT EXISTS rows_t ( 
		parent TEXT NOT NULL,
		table_id TEXT NOT NULL,
		row_key TEXT NOT NULL,
		families BYTEA NOT NULL,
		PRIMARY KEY (parent, table_id, row_key)
		)`
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	query = `CREATE TABLE IF NOT EXISTS tables_t (
		parent TEXT NOT NULL,
		table_id TEXT NOT NULL,
		metadata BYTEA NOT NULL,
		PRIMARY KEY  (parent, table_id)
		)`
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}
