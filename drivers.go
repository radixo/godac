package matilda

import (
	"database/sql"
	"errors"
	"reflect"
)

// DriverCreator function type
type DriverCreator func (Entity) Driver

// Driver interface type
type Driver interface {
}

// The default interface for writing a Relational Database Driver
type CRUDDriver interface {
	Insert(*sql.Tx, map[string]interface{}) error
	Update(*sql.Tx, map[string]interface{}) error
	SelectByKey(*sql.Tx, []string, ...interface{}) (map[string]interface{},
	    error)
	SelectOne(*sql.Tx, []string, string, ...interface{}) (
            map[string]interface{}, error)
	Select(*sql.Tx, []string, string, ...interface{}) (Rows, error)
	Delete(*sql.Tx, map[string]interface{}) error
}

// Map of registered DriverCreators for CRUD
var crudDrivers = make(map[string]DriverCreator)

// Register a CRUDDriver to be used by CRUD Entities
func RegisterCRUDDriver(libname string, c DriverCreator) {

	crudDrivers[libname] = c
}

// Get CRUDDriver
func GetCRUDDriver(e Entity) (drv CRUDDriver, err error) {

	connName := reflect.TypeOf(e.GetDB().Driver()).String()
	if d, ok := crudDrivers[connName]; ok == true {
		drv = d(e).(CRUDDriver)
		return
	}
	err = errors.New("Can't find a driver.")
	return
}
