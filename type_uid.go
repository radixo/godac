package matilda

import (
	"crypto/rand"
	sqldriver "database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

const uid_size = 16

// Unique Identifier created by matilda team
type UID struct {
	data []byte
}

func NewUID() (uid UID) {

	uid.data = make([]byte, uid_size)
	uid.Generate()
	return
}

func (uid *UID) Generate() {

	// Get time
	binary.LittleEndian.PutUint64(uid.data[:8], uint64(time.Now().Unix()))
	rand.Read(uid.data[8:])
	return
}

func (uid UID) String() string {

	return fmt.Sprintf("%x", uid.data)
}

func (uid *UID) ReadString(s string) error {

	// Clear uuid string
	s = strings.Replace(s, "-", "", -1)
	// Check size
	if len(s) != 32 {
		return errors.New("Wrong format for UID string")
	}
	// Decode to []byte
	dec, err := hex.DecodeString(s)
	if err != nil {
		return err
	}
	uid.data = make([]byte, uid_size)
	copy(uid.data, dec)

	return nil
}

func (uid *UID) Scan(src interface{}) error {
	var s string

	switch v := src.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprint(v)
	}

	err := uid.ReadString(s)
	if err != nil {
		return err
	}
	return nil
}

func (uid UID) Value() (sqldriver.Value, error) {

	return uid.data, nil
}

type VdrUID struct {
	NotNull bool
	Default interface{}
	AutoGen bool
}

func (vuid *VdrUID) ValidateField(data map[string]interface{}, fname string,
    state DataState) error {
	var val interface{}

	switch v := data[fname].(type) {
	case nil:
		val = v
		goto _assert
	case []byte, string:
		uid := UID{}
		if err := uid.Scan(v); err != nil {
			return err
		}
		if state == DS_LOADED {
			val = uid.String()
		} else {
			val = uid
		}
	case UID: val = v
	default:
		return fmt.Errorf("matilda: Field %q must be matilda.UID.", fname)
	}

	// assert options
	_assert:
	if vuid.AutoGen == true && val == nil {
		val = NewUID()
	}

	// assert type
	if vuid.Default != nil && val == nil {
		val = vuid.Default
	}
	if vuid.NotNull == true && val == nil {
		return fmt.Errorf("matilda: Field %q can't be null.", fname)
	}
	data[fname] = val
	return nil
}
