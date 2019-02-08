package drivers

import (
	"database/sql"

	"github.com/radixo/matilda"
)

type sqlRows struct {
	rows *sql.Rows
	entity matilda.Entity
	cols []string
	fieldValidators matilda.FieldValidatorsRunner
}

func getAutoIncColumn(e matilda.Entity) *matilda.Column {

	if e.GetType() == matilda.ENT_TABLE {
		for _, col := range e.(*matilda.Table).AllColumns {
			if col.AutoInc {
				return col
			}
		}
	}

	return nil
}

func SqlProcessExecResult(res sql.Result, e matilda.Entity,
    data map[string]interface{}) {
	var val int64
	var err error
	var col *matilda.Column

	if e.GetType() == matilda.ENT_TABLE {
		val, err = res.LastInsertId()
		if err != nil {
			goto endAutoInc
		}
		data[matilda.RES_AUTOINC] = val

		col = getAutoIncColumn(e)
		if col == nil {
			goto endAutoInc
		}

		data[col.Name] = val
	}
	endAutoInc:

	val, err = res.RowsAffected()
	if err != nil {
		goto endRowsAffected
	}
	data[matilda.RES_ROWSAFFECTED] = val

	// Made this way in case new functions appears on Result
	endRowsAffected:
}

func SqlProcessQueryRowResult(row *sql.Row, e matilda.Entity,
    cols []string) (map[string]interface{}, error) {
	var data = make([]interface{}, len(cols))
	var _data = make([]interface{}, len(cols))
	for i := range data {
		data[i] = &_data[i]
	}

	err := row.Scan(data...)
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	default:
		ret := make(map[string]interface{})
		for i := range cols {
			ret[cols[i]] = _data[i]
		}
		return ret, nil
	}
}

func SqlProcessQueryResult(rows *sql.Rows, e matilda.Entity,
    cols []string) matilda.Rows {

	r := new(sqlRows)
	r.rows = rows
	r.entity = e
	r.cols = cols
	return r
}

func (r *sqlRows) Close() {

	r.rows.Close()
}

func (r *sqlRows) Next() bool {

	return r.rows.Next()
}

func (r *sqlRows) Tuple() (map[string]interface{}, error) {
	var data = make([]interface{}, len(r.cols))
	var _data = make([]interface{}, len(r.cols))
	for i := range data {
		data[i] = &_data[i]
	}

	err := r.rows.Scan(data...)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]interface{})
	for i := range r.cols {
		ret[r.cols[i]] = _data[i]
	}

	if r.fieldValidators != nil {
		// Validate each field ignoring errors
		r.fieldValidators.RunFieldValidators(ret, matilda.DS_LOADED)
	}
	return ret, nil
}

func (r *sqlRows) SetFieldValidators(fvr matilda.FieldValidatorsRunner) {

	r.fieldValidators = fvr
}
