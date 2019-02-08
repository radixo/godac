package matilda

import (
	"fmt"
	"net/mail"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type DataState uint

const (
	DS_UNKNOW	DataState = iota
	DS_INSERT	DataState = iota
	DS_UPDATE	DataState = iota
	DS_LOADED	DataState = iota
)

type TableValidator interface {
	Validate(map[string]interface{}, DataState) error
}

type FieldValidator interface {
	ValidateField(map[string]interface{}, string, DataState) error
}

type VdrInt64 struct {
	NotNull bool
	Default interface{}
	Min, Max int64
	Timestamp bool
	PastOnly bool
	InsertUnixNow, UpdateUnixNow bool
}

func (vi64 *VdrInt64) ValidateField(data map[string]interface{}, fname string,
    state DataState) error {
	var err error
	var val interface{}
	var t time.Time

	switch v := data[fname].(type) {
	case nil:
		val = v
		goto _assert
	case int:
		val = int64(v)
	case int8:
		val = int64(v)
	case int16:
		val = int64(v)
	case int32:
		val = int64(v)
	case int64:
		val = v
	case string:
		if vi64.Timestamp == true {
			if t, err = time.Parse("2006-01-02", v); err == nil {
				val = t.Unix()
			} else if t, err := time.Parse("2006-01-02 15:04:05",
			    v); err == nil {
				val = t.Unix()
			} else if t, err := time.Parse(time.RFC3339, v);
			    err == nil {
				val = t.Unix()
			} else {
				return fmt.Errorf("Can't parse timestamp.")
			}
		} else {
			if val, err = strconv.ParseInt(v, 10, 64); err != nil {
				return fmt.Errorf("Can't parse int.")
			}
		}
	default:
		return fmt.Errorf("matilda: Field %q must be int64.", fname)
	}

	// check options
	if vi64.Min != 0 || vi64.Max != 0 {
		if val.(int64) < vi64.Min {
			return fmt.Errorf("matilda: %v is less then Min: %v.",
			    val, vi64.Min)
		}
		if val.(int64) > vi64.Max && vi64.Max > 0 {
			return fmt.Errorf("matilda: %v is bigger then Max: %v.",
			    val, vi64.Max)
		}
	}
	if vi64.PastOnly && val.(int64) > time.Now().Unix() {
		return fmt.Errorf("matilda: Field %q is not in the past.",
		    fname)
	}

	// assert options
	_assert:
	if vi64.InsertUnixNow == true && state == DS_INSERT {
		val = time.Now().Unix()
	}
	if vi64.UpdateUnixNow == true && state == DS_UPDATE {
		val = time.Now().Unix()
	}

	// assert type
	if vi64.Default != nil && val == nil {
		val = vi64.Default
	}
	if vi64.NotNull == true && val == nil {
		return fmt.Errorf("matilda: Field %q can't be null.", fname)
	}
	data[fname] = val
	return nil
}

type VdrInt32 struct {
	NotNull bool
	Default interface{}
	Min, Max int32
}

func (vi32 *VdrInt32) ValidateField(data map[string]interface{}, fname string,
    state DataState) error {
	var val interface{}

	switch v := data[fname].(type) {
	case nil:
		val = v
		goto _assert
	case int:
		val = int32(v)
	case int8:
		val = int32(v)
	case int16:
		val = int32(v)
	case int32:
		val = v
	default:
		return fmt.Errorf("matilda: Field %q must be int32.", fname)
	}

	// check options
	if vi32.Min != 0 || vi32.Max != 0 {
		if val.(int32) < vi32.Min {
			return fmt.Errorf("matilda: %v is less then Min: %v.",
			    val, vi32.Min)
		}
		if val.(int32) > vi32.Max && vi32.Max > 0 {
			return fmt.Errorf("matilda: %v is bigger then Max: %v.",
			    val, vi32.Max)
		}
	}

	// assert type
	_assert:
	if vi32.Default != nil && val == nil {
		val = vi32.Default
	}
	if vi32.NotNull == true && val == nil {
		return fmt.Errorf("matilda: Field %q can't be null.", fname)
	}
	data[fname] = val
	return nil
}

type VdrString struct {
	NotNull bool
	Default interface{}
	MinLen, MaxLen int
	MinWords, MaxWords int
	NoTrim bool
	Password, TrimPassword bool
}

func (vstr *VdrString) ValidateField(data map[string]interface{}, fname string,
    state DataState) error {
	var err error
	var val interface{}

	switch v := data[fname].(type) {
	case nil:
		val = v
		goto _assert
	case string:
		val = v
	case []byte:
		val = string(v)
	case fmt.Stringer:
		val = v.String()
	default:
		fmt.Printf("%T of %v\n", v, v)
		return fmt.Errorf("matilda: Field %q must be string.", fname)
	}

	if state != DS_INSERT && state != DS_UPDATE {
		goto _assert
	}

	// check options
	if vstr.Password == true && vstr.TrimPassword == false {
		vstr.NoTrim = true
	}
	if vstr.NoTrim == false {
		val = strings.TrimSpace(val.(string))
	}
	if vstr.MinLen != 0 || vstr.MaxLen != 0 {
		if len(val.(string)) < vstr.MinLen {
			return fmt.Errorf("matilda: Field %q %v is less then " +
			    "MinLen: %v.", fname, val, vstr.MinLen)
		}
		if len(val.(string)) > vstr.MaxLen && vstr.MaxLen > 0 {
			return fmt.Errorf("matilda: Field %q %v is bigger " +
			    "then Max: %v.", fname, val, vstr.MaxLen)
		}
	}
	if vstr.MinWords != 0 || vstr.MaxWords != 0 {
		_len := len(strings.Fields(val.(string)))
		if _len < vstr.MinWords {
			return fmt.Errorf("matilda: Field %q %v is less then " +
			    "MinWords: %v.", fname, val, vstr.MinLen)
		}
		if _len > vstr.MaxWords && vstr.MaxWords > 0 {
			return fmt.Errorf("matilda: Field %q %v is bigger " +
			    "then MaxWords: %v.", fname, val, vstr.MaxLen)
		}
	}

	// assert options
	if vstr.Password == true {
		if val, err = bcrypt.GenerateFromPassword([]byte(val.(string)),
		    10); err != nil {
			return err
		}
	}

	// assert type
	_assert:
	if vstr.Default != nil && val == nil {
		val = vstr.Default
	}
	if vstr.NotNull == true && val == nil {
		return fmt.Errorf("matilda: Field %q can't be null.", fname)
	}
	data[fname] = val
	return nil
}

type VdrBool struct {
	NotNull bool
	Default interface{}
}

func (vboo *VdrBool) ValidateField(data map[string]interface{}, fname string,
    state DataState) error {
	var val interface{}

	switch v := data[fname].(type) {
	case nil:
		val = v
		goto _assert
	case bool:
		val = v
	default:
		return fmt.Errorf("matilda: Field %q must be bool.", fname)
	}

	// assert type
	_assert:
	if vboo.Default != nil && val == nil {
		val = vboo.Default
	}
	if vboo.NotNull == true && val == nil {
		return fmt.Errorf("matilda: Field %q can't be null.", fname)
	}
	data[fname] = val
	return nil
}

type VdrEmailAddress struct {
	NotNull bool
}

func (vea *VdrEmailAddress) ValidateField(data map[string]interface{},
    fname string, state DataState) error {
	var val interface{}

	switch v := data[fname].(type) {
	case nil:
		val = v
		goto _assert
	case string:
		val = v
	default:
		return fmt.Errorf("matilda: Field %q must be string to " +
		    "validate email address.")
	}

	// check is valid
	if e, err := mail.ParseAddress(val.(string)); err != nil {
		return fmt.Errorf("matilda: Field %q with invalid email.",
		    fname)
	} else {
		val = e.Address
	}


	// assert type
	_assert:
	if vea.NotNull == true && val == nil {
		return fmt.Errorf("matilda: Field %q can't be null.", fname)
	}
	data[fname] = val
	return nil
}
