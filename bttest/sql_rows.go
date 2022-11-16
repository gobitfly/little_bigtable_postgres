package bttest

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/google/btree"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

// SqlRows is a backend modeled on the github.com/google/btree interface
// all errors are considered fatal
//
// rows are persisted in rows_t
type SqlRows struct {
	parent  string // Values are of the form `projects/{project}/instances/{instance}`.
	tableId string // The name by which the new table should be referred to within the parent instance

	mu sync.RWMutex
	db *sqlx.DB
}

func NewSqlRows(db *sqlx.DB, parent, tableId string) *SqlRows {
	return &SqlRows{
		parent:  parent,
		tableId: tableId,
		db:      db,
	}
}

func (r *row) Scan(src interface{}) error {
	switch src := src.(type) {
	case nil:
		return nil
	case []byte:
	default:
		return fmt.Errorf("unknown type %T", src)
	}
	return msgpack.Unmarshal(src.([]byte), &r.families)
}
func (r *row) Bytes() ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	return msgpack.Marshal(&r.families)
}

type ItemIterator = btree.ItemIterator
type Item = btree.Item

func (db *SqlRows) query(iterator ItemIterator, query string, args ...interface{}) {
	// db.mu.RLock()
	// defer db.mu.RUnlock()
	rows, err := db.db.Query(query, args...)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		logrus.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.key, &r); err != nil {
			logrus.Fatal(err)
		}
		if !iterator(&r) {
			break
		}
	}
	if err := rows.Err(); err != nil {
		logrus.Fatal(err)
	}
}

func (db *SqlRows) Ascend(iterator ItemIterator) {
	start := time.Now()
	logrus.Infof("Ascend for table %s/%s", db.parent, db.tableId)
	defer func() {
		logrus.Infof("Ascend for table %s/%s completed in %v", db.parent, db.tableId, time.Since(start))
	}()

	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = $1 and table_id = $2 ORDER BY row_key ASC", db.parent, db.tableId)
}

func (db *SqlRows) AscendGreaterOrEqual(pivot Item, iterator ItemIterator) {
	row := pivot.(*row)

	start := time.Now()
	logrus.Infof("AscendGreaterOrEqual for %s of table %s/%s", row.key, db.parent, db.tableId)
	defer func() {
		logrus.Infof("AscendGreaterOrEqual for %s of table %s/%s completed in %v", row.key, db.parent, db.tableId, time.Since(start))
	}()

	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = $1 and table_id = $2 and row_key >= $3 ORDER BY row_key ASC", db.parent, db.tableId, row.key)
}

func (db *SqlRows) AscendLessThan(pivot Item, iterator ItemIterator) {
	row := pivot.(*row)

	start := time.Now()
	logrus.Infof("AscendLessThan for %s of table %s/%s", row.key, db.parent, db.tableId)
	defer func() {
		logrus.Infof("AscendLessThan for %s of table %s/%s completed in %v", row.key, db.parent, db.tableId, time.Since(start))
	}()

	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = $1 and table_id = $2 and row_key < $3 ORDER BY row_key ASC", db.parent, db.tableId, row.key)
}

func (db *SqlRows) AscendRange(greaterOrEqual, lessThan Item, iterator ItemIterator) {
	ge := greaterOrEqual.(*row)
	lt := lessThan.(*row)

	start := time.Now()
	logrus.Infof("AscendRange for %s/%s of table %s/%s", ge.key, lt.key, db.parent, db.tableId)
	defer func() {
		logrus.Infof("AscendRange for %s/%s of table %s/%s completed in %v", ge.key, lt.key, db.parent, db.tableId, time.Since(start))
	}()

	db.query(iterator, "SELECT row_key, families FROM rows_t WHERE parent = $1 and table_id = $2 and row_key >= $3 and row_key < $4 ORDER BY row_key ASC", db.parent, db.tableId, ge.key, lt.key)
}

func (db *SqlRows) DeleteAll() {
	db.mu.Lock()
	defer db.mu.Unlock()

	start := time.Now()
	logrus.Infof("DeleteAll for table %s/%s", db.parent, db.tableId)
	defer func() {
		logrus.Infof("DeleteAll for table %s/%s completed in %v", db.parent, db.tableId, time.Since(start))
	}()

	_, err := db.db.Exec("DELETE FROM rows_t WHERE parent = $1 and table_id = $2", db.parent, db.tableId)
	if err != nil {
		logrus.Fatal(err)
	}

}

func (db *SqlRows) Delete(item Item) {
	db.mu.Lock()
	defer db.mu.Unlock()
	row := item.(*row)

	start := time.Now()
	logrus.Infof("Delete for %s of table %s/%s", row.key, db.parent, db.tableId)
	defer func() {
		logrus.Infof("Delete for %s of table %s/%s completed in %v", row.key, db.parent, db.tableId, time.Since(start))
	}()

	_, err := db.db.Exec("DELETE FROM rows_t WHERE parent = $1 and table_id = $2 and row_key = $3", db.parent, db.tableId, row.key)
	if err != nil {
		logrus.Fatal(err)
	}
}

func (db *SqlRows) Get(key Item) Item {
	row := key.(*row)
	if row.families == nil {
		row.families = make(map[string]*family)
	}
	// db.mu.RLock()
	// defer db.mu.RUnlock()

	start := time.Now()
	logrus.Infof("Get for %s of table %s/%s", row.key, db.parent, db.tableId)
	defer func() {
		logrus.Infof("Get for %s of table %s/%s completed in %v", row.key, db.parent, db.tableId, time.Since(start))
	}()

	err := db.db.QueryRow("SELECT families FROM rows_t WHERE parent = $1 and table_id = $2 and row_key = $3", db.parent, db.tableId, row.key).Scan(row)
	if err == sql.ErrNoRows {
		return row
	}
	if err != nil {
		logrus.Fatal(err)
	}
	return row
}

func (db *SqlRows) Len() int {
	var count int
	// db.mu.RLock()
	// defer db.mu.RUnlock()

	start := time.Now()
	logrus.Infof("Len for table %s/%s", db.parent, db.tableId)
	defer func() {
		logrus.Infof("Len for table %s/%s completed in %v", db.parent, db.tableId, time.Since(start))
	}()

	err := db.db.QueryRow("SELECT count(*) FROM rows_t WHERE parent = $1 and table_id = $2", db.parent, db.tableId).Scan(&count)
	if err != nil {
		logrus.Fatal(err)
	}
	return count
}

func (db *SqlRows) ReplaceOrInsert(item Item) Item {
	row := item.(*row)
	families, err := row.Bytes()
	if err != nil {
		logrus.Fatal(err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	start := time.Now()
	logrus.Infof("ReplaceOrInsert for %s of table %s/%s", row.key, db.parent, db.tableId)
	defer func() {
		logrus.Infof("ReplaceOrInsert for %s of table %s/%s completed in %v", row.key, db.parent, db.tableId, time.Since(start))
	}()

	_, err = db.db.Exec("INSERT INTO rows_t (parent, table_id, row_key, families) values ($1, $2, $3, $4) ON CONFLICT (parent, table_id, row_key) DO UPDATE SET families = EXCLUDED.families", db.parent, db.tableId, row.key, families)
	if err != nil {
		logrus.Fatalf("row:%s err %s", row.key, err)
	}
	return row
}
