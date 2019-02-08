package postgres

import (

	"github.com/radixo/matilda"
)

var knownDrivers = []string {
	"*pq.Driver",
}

func init() {

	// Drivers registration
	for _, dname := range knownDrivers {
		matilda.RegisterCRUDDriver(dname, NewCRUDDriver)
	}
}
