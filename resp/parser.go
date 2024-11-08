package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type decoderState struct {
	dataToDecode []byte
	scanner      *bufio.Scanner
	typeToDecode byte
	typeLength   int
	offset       int
}

const (
	RESP_simple_string    = '+'
	RESP_simple_error     = '-'
	RESP_integer          = ':'
	RESP_bulk_string      = '$'
	RESP_array            = '*'
	RESP_null             = '_'
	RESP_boolean          = '#'
	RESP_doubles          = ','
	RESP_bignum           = '('
	RESP_bulk_errors      = '!'
	RESP_verbatim_strings = '='
	RESP_map              = '%'
	RESP_attribute        = '`'
	RESP_set              = '~'
	RESP_push             = '>'
)

func (d *decoderState) scanNext() bool {
	// wrapper to scanner.Scan() calls.
	// need to update offset value every time scanner.Scan() called

	moreLinesExist := d.scanner.Scan()
	d.offset = len(d.scanner.Bytes()) + 2 // bytes read + CRLF + first unread index
	return moreLinesExist
}

func newDecoderState(dataToDecode []byte) *decoderState {
	scanner := bufio.NewScanner(bytes.NewReader(dataToDecode))
	scanner.Split(ScanCRLF)
	return &decoderState{
		dataToDecode: dataToDecode,
		scanner:      scanner,
	}
}

func Marshal(v any) (string, error) {

	// var sb strings.Builder
	// get data type
	reflectValue := reflect.ValueOf(v)

	return reflectValueToRESP(reflectValue, false)
}

func Unmarshal(data []byte, v any) error {

	if len(data) < 2 {
		return fmt.Errorf("unknown RESP protocol: %s", string(data))
	}

	ds := newDecoderState(data)
	err := ds.unpack(v)
	if err != nil {
		return fmt.Errorf("error unpacking values: %s", err.Error())
	}

	return nil
}

func (d *decoderState) unpack(v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("error decoding RESP data: got %s. nil: %t", reflect.TypeOf(v).Kind(), rv.IsNil())
	}

	if d.scanNext() {
		bytesContent := d.scanner.Bytes()
		// switch on type
		switch bytesContent[0] {
		case RESP_array:
			res, err := strconv.Atoi(string(bytesContent[1:]))
			d.typeLength = res
			if err != nil {
				return fmt.Errorf("cannot get array length from %s", bytesContent)
			}
			err = d.unpackArray(v)
			if err != nil {
				return fmt.Errorf("error decoding array: %s", err)
			}
			return nil
		case RESP_simple_string:
			rve := rv.Elem()
			if rve.Kind() != reflect.String {
				return fmt.Errorf("cant use this pointer: %s. it's element's type is %s. expected type: %s", v, rve.Type().String(), reflect.String.String())
			}
			if !rve.CanSet() {
				return fmt.Errorf("cannot change element %s pointer referred to", v)
			}
			rv.Elem().SetString(string(bytesContent[1:]))
		case RESP_bulk_string:
			_, err := strconv.Atoi(string(bytesContent[1:]))
			if err != nil {
				return fmt.Errorf("cannot get bulk string length from %s: %s", string(bytesContent[1:]), err)
			}
			// scan to next CRLF
			d.scanNext()
			strContent := d.scanner.Bytes()
			rve := rv.Elem()
			if rve.Kind() != reflect.String {
				return fmt.Errorf("cant use this pointer: %s. it's element's type is %s. expected type: %s", v, rve.Type().String(), reflect.String.String())
			}
			if !rve.CanSet() {
				return fmt.Errorf("cannot change element %s pointer referred to", v)
			}
			rv.Elem().SetString(string(strContent))
		case RESP_integer:
			d.scanNext()
			intContent, err := strconv.Atoi(string(d.scanner.Bytes()))
			if err != nil {
				return fmt.Errorf("cant convert %s to integer: %s", string(d.scanner.Bytes()), err)
			}
			rve := rv.Elem()
			if rve.Kind() != reflect.Int {
				return fmt.Errorf("cant use this pointer: %s. it's element's type is %s. expected type: %s", v, rve.Type().String(), reflect.Int.String())
			}
			if !rve.CanSet() {
				return fmt.Errorf("cannot change element %s pointer referred to", v)
			}
			rv.Elem().SetInt(int64(intContent))
		}
		return nil
	}

	return nil
}

func (d *decoderState) unpackArray(v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("error decoding RESP data: got %s. nil: %t", reflect.TypeOf(v).Kind(), rv.IsNil())
	}
	arrElementType := []byte{}
	for i := d.offset; i < len(d.dataToDecode); i++ {
		// read until CRLF
		if d.dataToDecode[i] == '\r' && d.dataToDecode[i+1] == '\n' {
			break
		}
		arrElementType = append(arrElementType, d.dataToDecode[i])
	}

	rve := rv.Elem()
	rve.Grow(d.typeLength)

	switch arrElementType[0] {
	case RESP_simple_string:
		rve.Set(reflect.ValueOf(make([]string, d.typeLength)))
		// loop through elements
		for i := 0; i < d.typeLength; i++ {
			elem := rve.Index(i)
			d.unpack(elem.Addr().Interface().(*string))
		}
	case RESP_bulk_string:
		rve.Set(reflect.ValueOf(make([]string, d.typeLength)))
		for i := 0; i < d.typeLength; i++ {
			elem := rve.Index(i)
			d.unpack(elem.Addr().Interface().(*string))
		}
	case RESP_integer:
		rve.Set(reflect.ValueOf(make([]int, d.typeLength)))
		for i := 0; i < d.typeLength; i++ {
			elem := rve.Index(i)
			d.unpack(elem.Addr().Interface().(*int))
		}
	}

	return nil

}

func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

func ScanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, []byte{'\r', '\n'}); i >= 0 {
		return i + 2, dropCR(data[0:i]), nil
	}
	if atEOF {
		return len(data), dropCR(data), nil
	}
	return 0, nil, nil
}

func reflectValueToRESP(v reflect.Value, mapKey bool) (string, error) {
	var sb strings.Builder
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Int:
		return fmt.Sprintf(":%d\r\n", v.Int()), nil
	case reflect.Float64:
		return fmt.Sprintf(",%f\r\n", v.Float()), nil
	case reflect.String:
		if mapKey {
			return fmt.Sprintf("+%s\r\n", v.String()), nil
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", v.Len(), v.String()), nil
	case reflect.Map:
		sb.WriteString(fmt.Sprintf("%%%d\r\n", v.Len()))
		iter := v.MapRange()
		for iter.Next() {
			k := iter.Key()
			v := iter.Value()
			kS, err := reflectValueToRESP(k, true)
			if err != nil {
				return "", fmt.Errorf("failed to convert data: %s is unconvertable to RESP protocol", k)
			}
			sb.WriteString(kS)
			vS, err := reflectValueToRESP(v, false)
			if err != nil {
				return "", fmt.Errorf("failed to convert data: %s is unconvertable to RESP protocol", v)
			}
			sb.WriteString(vS)
		}
	case reflect.Slice, reflect.Array:
		sb.WriteString(fmt.Sprintf("*%d\r\n", v.Len()))
		for i := 0; i < v.Len(); i++ {
			res, err := reflectValueToRESP(v.Index(i), false)
			if err != nil {
				return "", fmt.Errorf("failed to convert data: %s is unconvertable to RESP protocol", v.Index(i))
			}
			sb.WriteString(res)
		}
		return sb.String(), nil
	}
	return "", nil
}
