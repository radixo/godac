package postgres

import (

	"github.com/radixo/matilda"
)

type PgCRUDDriver struct {
	// Entity type
	etype matilda.EntityType

	// Pointer to table being used by the driver
	table *matilda.Table
}
