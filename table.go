package matilda

import (
	"database/sql"
	"fmt"
)

type Table struct {
	// The parent struct
	parent interface{}

	// Table name on database
	Name string

	// Table columns
	AllColumns []*Column

	// Table columns
	Columns []*Column

	// Table primary keys
	PKeys []*Column

	// Database connection
	db *sql.DB

	// Database driver
	drv CRUDDriver
}

func newTable(parent interface{}, db *sql.DB, name string, cols ...*Column) (
    t *Table) {

	t = new(Table)
	t.parent = parent
	t.SetDB(db)
	t.Name = name
	for _, col := range cols {
		t.addCol(col)
	}

	return t
}

func (t *Table) addCol(col *Column) {

	if col.PKey {
		t.PKeys = append(t.PKeys, col)
	} else {
		t.Columns = append(t.Columns, col)
	}
	t.AllColumns = append(t.AllColumns, col)
}

func (t *Table) GetType() EntityType {

	return ENT_TABLE
}

func (t *Table) GetDB() *sql.DB {

	return t.db
}

func (t *Table) SetDB(db *sql.DB) {
	var err error

	if db == nil {
		t.db = nil
		t.drv = nil
		return
	}

	t.db = db
	if t.drv, err = GetCRUDDriver(t); err != nil {
		panic(err)
	}
	return
}

func (t *Table) mergeWithDB(data map[string]interface{}) error {
	var keys []interface{}
	var cols []string

	for _, col := range t.PKeys {
		if _, ok := data[col.Name]; ok == false {
			return fmt.Errorf("key %q not present in data.",
			    col.Name)
		}
		keys = append(keys, data[col.Name])
	}

	for _, col := range t.Columns {
		if _, ok := data[col.Name]; ok == false {
			cols = append(cols, col.Name)
		}
	}

	if len(cols) == 0 {
		// We have all columns
		return nil
	}

	rows, err := t.SelectByKey(cols, keys...)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		// We do not have data
		return fmt.Errorf("Record not found.")
	}

	for mkey, mval := range rows {
		data[mkey] = mval
	}
	return nil
}

type FieldValidatorsRunner interface {
	RunFieldValidators(map[string]interface{}, DataState) error
}

func (t *Table) RunFieldValidators(data map[string]interface{},
    ds DataState) error {

	// Validate each field
	for _, col := range t.AllColumns {
		if _, ok := data[col.Name]; ok == false && ds == DS_LOADED {
			continue
		}

		for _, vdr := range col.Validators {
			if err := vdr.ValidateField(data, col.Name, ds);
			    err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Table) RunValidators(data map[string]interface{}, ds DataState) error {

	// Merge with db version
	if ds == DS_UPDATE {
		err := t.mergeWithDB(data)
		if err != nil {
			return err
		}
	}

	// Validate each field
	if err := t.RunFieldValidators(data, ds); err != nil {
		return err
	}

	if validator, ok := t.parent.(TableValidator); ok == true {
		if err := validator.Validate(data, ds); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) Insert(data map[string]interface{}) error {

	return t.InsertTx(nil, data)
}

func (t *Table) InsertTx(tx *sql.Tx, data map[string]interface{}) error {

	if err := t.RunValidators(data, DS_INSERT); err != nil {
		return err
	}
	return t.drv.Insert(tx, data)
}

func (t *Table) Update(data map[string]interface{}) error {

	return t.UpdateTx(nil, data)
}

func (t *Table) UpdateTx(tx *sql.Tx, data map[string]interface{}) error {

	if err := t.RunValidators(data, DS_UPDATE); err != nil {
		return err
	}
	return t.drv.Update(tx, data)
}

func (t *Table) SelectByKey(cols []string, keys ...interface{}) (
    map[string]interface{}, error) {

	return t.SelectByKeyTx(nil, cols, keys...)
}

func (t *Table) SelectByKeyTx(tx *sql.Tx, cols []string, keys ...interface{}) (
    map[string]interface{}, error) {

	data, err := t.drv.SelectByKey(tx, cols, keys...)

	// Validate each field ignoring errors
	t.RunFieldValidators(data, DS_LOADED)

	return data, err
}

func (t *Table) SelectOne(cols []string, filter string,
    params ...interface{}) (map[string]interface{}, error) {

	return t.SelectOneTx(nil, cols, filter, params...)
}

func (t *Table) SelectOneTx(tx *sql.Tx, cols []string, filter string,
    params ...interface{}) (map[string]interface{}, error) {

	data, err := t.drv.SelectOne(tx, cols, filter, params...)

	// Validate each field ignoring errors
	t.RunFieldValidators(data, DS_LOADED)

	return data, err
}

func (t *Table) Select(cols []string, filter string, params ...interface{}) (
    Rows, error) {

	return t.SelectTx(nil, cols, filter, params...)
}

func (t *Table) SelectTx(tx *sql.Tx, cols []string, filter string,
    params ...interface{}) (Rows, error) {

	rows, err := t.drv.Select(tx, cols, filter, params...)
	// For field validation
	rows.SetFieldValidators(t)
	return rows, err
}

func (t *Table) Delete(data map[string]interface{}) error {

	return t.DeleteTx(nil, data)
}

func (t *Table) DeleteTx(tx *sql.Tx, data map[string]interface{}) error {

	if err := t.drv.Delete(tx, data); err != nil {
		return err
	}

	if data[RES_ROWSAFFECTED] == int64(0) {
		return fmt.Errorf("Record not found.")
	}
	return nil
}

func (t *Table) AddCol(name string, vdrs ...FieldValidator) {

	t.addCol(NewCol(name, vdrs...))
}

func (t *Table) AddColAutoInc(name string, vdrs ...FieldValidator) {

	t.addCol(NewColAutoInc(name, vdrs...))
}

func (t *Table) AddColPK(name string, vdrs ...FieldValidator) {

	t.addCol(NewColPK(name, vdrs...))
}

func (t *Table) AddColAutoIncPK(name string, vdrs ...FieldValidator) {

	t.addCol(NewColAutoIncPK(name, vdrs...))
}
