package bttest

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

// SqlTables persists tables to tables_t
type SqlTables struct {
	db *sqlx.DB
}

func NewSqlTables(db *sqlx.DB) *SqlTables {
	return &SqlTables{
		db: db,
	}
}

func (t *table) Scan(src interface{}) error {
	switch src := src.(type) {
	case nil:
		return nil
	case []byte:
	default:
		return fmt.Errorf("unknown type %T", src)
	}

	err := msgpack.Unmarshal(src.([]byte), &t.families)

	if err != nil {
		return err
	}

	t.counter = uint64(len(t.families))
	for _, f := range t.families {
		if f.Order > t.counter {
			t.counter = f.Order
		}
	}
	return err
}

func (t *table) Bytes() ([]byte, error) {
	if t == nil {
		return nil, nil
	}
	return msgpack.Marshal(&t.families)
}

func (db *SqlTables) Get(parent, tableId string) *table {
	tbl := &table{
		parent:  parent,
		tableId: tableId,
		rows:    NewSqlRows(db.db, parent, tableId),
	}
	err := db.db.QueryRow("SELECT metadata FROM tables_t WHERE parent = $1 AND table_id = $2", parent, tableId).Scan(tbl)
	if err == sql.ErrNoRows {
		return nil
	}
	return tbl
}

func (db *SqlTables) GetAll() []*table {
	var tables []*table

	rows, err := db.db.Query("SELECT parent, table_id, metadata FROM tables_t")
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		logrus.Fatalf("SqlTables GetAll: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		t := table{}
		if err := rows.Scan(&t.parent, &t.tableId, &t); err != nil {
			logrus.Fatalf("SqlTables GetAll Scan: %v", err)
		}
		t.rows = NewSqlRows(db.db, t.parent, t.tableId)
		tables = append(tables, &t)
	}
	if err := rows.Err(); err != nil {
		logrus.Fatalf("SqlTables GetAll Err: %v", err)
	}
	return tables
}

func (db *SqlTables) Save(t *table) {
	metadata, err := t.Bytes()
	if err != nil {
		logrus.Fatal(err)
	}
	_, err = db.db.Exec("INSERT INTO tables_t (parent, table_id, metadata) VALUES ($1, $2, $3) ON CONFLICT (parent, table_id) DO UPDATE SET metadata = EXCLUDED.metadata", t.parent, t.tableId, metadata)
	if err != nil {
		logrus.Fatalf("SqlTables Save: %v", err)
	}
}

func (db *SqlTables) Delete(t *table) {
	_, err := db.db.Exec("DELETE FROM tables_t WHERE parent = $1 AND table_id = $2 ", t.parent, t.tableId)
	if err != nil {
		logrus.Fatalf("SqlTables Delete: %v", err)
	}
}
