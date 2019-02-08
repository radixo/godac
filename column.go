package matilda

type Column struct {
	// Column name on database
	Name string

	// Column type on database
	Typ string

	// Is a primary key
	PKey bool

	// Is auto incremental
	AutoInc bool

	// FieldValidators
	Validators []FieldValidator
}

func NewCol(name string, vdrs ...FieldValidator) (c *Column) {

	c = new(Column)
	c.Name = name
	c.Validators = vdrs

	return c
}

func NewColAutoInc(name string, vdrs ...FieldValidator) (c *Column) {

	c = NewCol(name, vdrs...)
	c.AutoInc = true

	return c
}

func NewColPK(name string, vdrs ...FieldValidator) (c *Column) {

	c = NewCol(name, vdrs...)
	c.PKey = true

	return c
}

func NewColAutoIncPK(name string, vdrs ...FieldValidator) (c *Column) {

	c = NewCol(name, vdrs...)
	c.AutoInc = true
	c.PKey = true

	return c
}
