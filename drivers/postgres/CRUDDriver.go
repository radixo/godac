package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/radixo/matilda"
	"github.com/radixo/matilda/drivers"
)

func NewCRUDDriver(e matilda.Entity) matilda.Driver {
	var d = new(PgCRUDDriver)

	d.etype = e.GetType()

	// Stores table reference into the driver instance
	if d.etype == matilda.ENT_TABLE {
		d.table = e.(*matilda.Table)
	}
	return d
}

func assureIdentifier(s string) string {
	var n = len(s)

	if n > 2 && s[0] == '"' && s[n-1] == '"' {
		return s
	}
	return strconv.Quote(s)
}

func paramsString(n int) string {
	var s []string

	if n <= 0 {
		return ""
	}

	s = make([]string, n)
	for i := 0; i < n; i++ {
		s[i] = "$" + strconv.Itoa(i + 1)
	}

	return strings.Join(s, ",")
}

func paramsEqual(cols []string, i *int) (ret []string) {

	for _, col := range cols {
		*i++
		ret = append(ret, col + " = $" + strconv.Itoa(*i))
	}
	return
}

func (p *PgCRUDDriver) assureIdentifiers(cols []string) (n, o []string) {

	if cols != nil {
		goto endAllColumns
	}
	// Using all columns
	for _, scol := range p.table.AllColumns {
		n = append(n, assureIdentifier(scol.Name))
		o = append(o, scol.Name)
	}
	return
	endAllColumns:

	// Using selected columns
	for _, col := range cols {
		n = append(n, assureIdentifier(col)) // new
		o = append(o, col) // old
	}
	return
}

func (p *PgCRUDDriver) pkeysIdentifiers() (cols []string) {

	for _, col := range p.table.PKeys {
		cols = append(cols, assureIdentifier(col.Name))
	}
	return
}

func assureVal(val interface{}) interface{} {

	switch v := val.(type) {
	case matilda.UID:
		return v.String()
	default:
		return val
	}
}

func assureVals(vals []interface{}) []interface{} {

	for i := range vals {
		vals[i] = assureVal(vals[i])
	}
	return vals
}

func assureCols(ref []*matilda.Column, data map[string]interface{}) (
    cols []string, vals []interface{}) {

	for _, col := range ref {
		if val, ok := data[col.Name]; ok == true {
			cols = append(cols, assureIdentifier(col.Name))
			vals = append(vals, assureVal(val))
		}
	}

	return
}

func (p *PgCRUDDriver) assureAllColumns(data map[string]interface{}) (
    []string, []interface{}) {

	return assureCols(p.table.AllColumns, data);
}

func (p *PgCRUDDriver) assureColumns(data map[string]interface{}) (
    []string, []interface{}) {

	return assureCols(p.table.Columns, data);
}

func (p *PgCRUDDriver) assurePKeys(data map[string]interface{}) (
    []string, []interface{}) {

	return assureCols(p.table.PKeys, data);
}

func (p *PgCRUDDriver) Insert(tx *sql.Tx, data map[string]interface{}) error {
	var err error
	var res sql.Result

	switch p.etype {
	case matilda.ENT_TABLE:
		cols, vals := p.assureAllColumns(data)
		sql := fmt.Sprintf("INSERT INTO %s(%s)VALUES(%s);",
		    assureIdentifier(p.table.Name), strings.Join(cols, ","),
		    paramsString(len(cols)))
		if tx == nil {
			res, err = p.table.GetDB().Exec(sql, vals...)
		} else {
			res, err = tx.Exec(sql, vals...)
		}
		if err != nil {
			return errors.New("matilda driver Insert: " + err.Error())
		}
		drivers.SqlProcessExecResult(res, p.table, data)
	default:
		return errors.New("Entity type not implemented.")
	}

	return nil
}

func (p *PgCRUDDriver) Update(tx *sql.Tx, data map[string]interface{}) error {
	var err error
	var res sql.Result

	switch p.etype {
	case matilda.ENT_TABLE:
		i := new(int)
		cols, vals := p.assureColumns(data)
		p_cols, p_vals := p.assurePKeys(data)
		sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s;",
		    assureIdentifier(p.table.Name),
		    strings.Join(paramsEqual(cols, i), ","),
		    strings.Join(paramsEqual(p_cols, i), " AND "))
		if tx == nil {
			res, err = p.table.GetDB().Exec(sql,
			    append(vals, p_vals...)...)
		} else {
			res, err = tx.Exec(sql,
			    append(vals, p_vals...)...)
		}
		if err != nil {
			return errors.New("matilda driver Update: " +
			    err.Error())
		}
		drivers.SqlProcessExecResult(res, p.table, data)
	default:
		return errors.New("Entity type not implemented.")
	}

	return nil
}

func (p *PgCRUDDriver) SelectByKey(tx *sql.Tx, cols []string,
    keys ...interface{}) (map[string]interface{}, error) {
	var row *sql.Row

	switch p.etype {
	case matilda.ENT_TABLE:
		i := new(int)
		a_cols, _cols := p.assureIdentifiers(cols)
		p_cols := p.pkeysIdentifiers()
		sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s;",
		    strings.Join(a_cols, ","),
		    assureIdentifier(p.table.Name),
		    strings.Join(paramsEqual(p_cols, i), " AND "))

		if tx == nil {
			row = p.table.GetDB().QueryRow(sql,
			    assureVals(keys)...)
		} else {
			row = tx.QueryRow(sql, assureVals(keys)...)
		}

		ret, err := drivers.SqlProcessQueryRowResult(row, p.table,
		    _cols)
		if err != nil {
			return nil, errors.New("matilda driver SelectByKey: " +
			    err.Error())
		}
		return ret, nil
	default:
		return nil, errors.New("Entity type not implemented.")
	}
}

func (p *PgCRUDDriver) SelectOne(tx *sql.Tx, cols []string, filter string,
    params ...interface{}) (map[string]interface{}, error) {
	var row *sql.Row

	switch p.etype {
	case matilda.ENT_TABLE:
		a_cols, _cols := p.assureIdentifiers(cols)
		if filter == "" {
			filter = "1=1"
		}
		sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s;",
		    strings.Join(a_cols, ","),
		    assureIdentifier(p.table.Name),
		    filter)

		if tx == nil {
			row = p.table.GetDB().QueryRow(sql,
			    assureVals(params)...)
		} else {
			row = tx.QueryRow(sql, assureVals(params)...)
		}

		ret, err := drivers.SqlProcessQueryRowResult(row, p.table,
		    _cols)
		if err != nil {
			return nil, errors.New("matilda driver SelectOne: " +
			    err.Error())
		}
		return ret, nil
	default:
		return nil, errors.New("Entity type not implemented.")
	}
}

func (p *PgCRUDDriver) Select(tx *sql.Tx, cols []string, filter string,
    params ...interface{}) (matilda.Rows, error) {
	var err error
	var rows *sql.Rows

	switch p.etype {
	case matilda.ENT_TABLE:
		a_cols, _cols := p.assureIdentifiers(cols)
		if filter == "" {
			filter = "1=1"
		}
		sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s;",
		    strings.Join(a_cols, ","),
		    assureIdentifier(p.table.Name),
		    filter)
		if tx == nil {
			rows, err = p.table.GetDB().Query(sql,
			    assureVals(params)...)
		} else {
			rows, err = tx.Query(sql, assureVals(params)...)
		}
		if err != nil {
			return nil, errors.New("matilda driver Select: " +
			    err.Error())
		}
		ret := drivers.SqlProcessQueryResult(rows, p.table, _cols)
		return ret, nil
	default:
		return nil, errors.New("Entity type not implemented.")
	}
}

func (p *PgCRUDDriver) Delete(tx *sql.Tx, data map[string]interface{}) error {
	var err error
	var res sql.Result

	switch p.etype {
	case matilda.ENT_TABLE:
		i := new(int)
		p_cols, p_vals := p.assurePKeys(data)
		sql := fmt.Sprintf("DELETE FROM %s WHERE %s;",
		    assureIdentifier(p.table.Name),
		    strings.Join(paramsEqual(p_cols, i), " AND "))
		if tx == nil {
			res, err = p.table.GetDB().Exec(sql, p_vals...)
		} else {
			res, err = tx.Exec(sql, p_vals...)
		}
		if err != nil {
			return errors.New("matilda driver Delete: " +
			    err.Error())
		}
		drivers.SqlProcessExecResult(res, p.table, data)
	default:
		return errors.New("Entity type not implemented.")
	}

	return nil
}
