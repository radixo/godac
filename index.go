package matilda

// Create a type name for index types
type IdxType uint
const (
	PKEY IdxType = iota
	INDX
)

type Index struct {
	// Index name on database
	name string

	// Index type
	typ IdxType

	// Index columns
	columns map[int]string

	// Index column asc
	asc map[int]bool
}
