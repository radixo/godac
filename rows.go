package matilda

type Rows interface {
	Next() bool
	Tuple() (map[string]interface{}, error)
	Close()
	SetFieldValidators(FieldValidatorsRunner)
}
