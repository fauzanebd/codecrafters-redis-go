package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"reflect"
	"strings"
)

func Marshal(v any) (string, error) {

	// var sb strings.Builder
	// get data type
	reflectValue := reflect.ValueOf(v)
	fmt.Println("tipe var: ", reflectValue.Type())
	return reflectValueToRESP(reflectValue, false)
}

func Unmarshal(data []byte, v any) error {

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("error decoding RESP data: got %s. nil: %t", reflect.TypeOf(v).Kind(), rv.IsNil())
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Split(ScanCRLF)

	switch data[0] {
	case '*':
		// * - array/slice
		for scanner.Scan() {
			fmt.Println("got: ", scanner.Text())
		}

	case '+':
		// + - simple string
	case '$':
		// $ - bulk string
	case '%':
		// % - map
	case ',':
		// , - float
	case ':':
		// : - int
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
