package matilda

import (
	"database/sql"
)

// Entity type
type EntityType int
const (
	ENT_TABLE EntityType = iota
)

// Entity interface type
type Entity interface {
	GetDB() *sql.DB
	GetType() EntityType
}
