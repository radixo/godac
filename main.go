package matilda

import (
	"database/sql"
)

func NewTable(parent interface{}, db *sql.DB, name string, cols ...*Column) (
    t *Table) {

	return newTable(parent, db, name, cols...)
}
