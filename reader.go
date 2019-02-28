package tsv

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unsafe"
)

// Reader is a zeek tsv file reader.
type Reader struct {
	parser *Parser
	header *Header
}

// Header is a zeek tsv file header.
type Header struct {
	Separator    byte
	Fields       []string
	Types        []FieldType
	Unset        []byte
	Empty        []byte
	SetSeparator []byte
}

// FieldType is a zeek field type.
type FieldType struct {
	dataType  DataType
	container bool
}

// DataType is a zeek data type.
type DataType int

// Enum of zeek data types.
const (
	String DataType = iota
	Time
	Addr
	Port
	Int
	Double
	Count
	Interval
	Bool
	Enum
)

// Map from #types to DataTypes.
var dataTypeLookup = map[string]DataType{
	"string":   String,
	"time":     Time,
	"addr":     Addr,
	"port":     Port,
	"int":      Int,
	"double":   Double,
	"count":    Count,
	"interval": Interval,
	"bool":     Bool,
	"enum":     Enum,
}

// ValueConverters maps DataTypes to converter functions.
var ValueConverters [10]func(b []byte) (interface{}, error)

func init() {
	ValueConverters[String] = toString
	ValueConverters[Time] = toString
	ValueConverters[Addr] = toString
	ValueConverters[Port] = toUint16
	ValueConverters[Int] = toUint64
	ValueConverters[Double] = toFloat64
	ValueConverters[Count] = toUint64
	ValueConverters[Interval] = toFloat64
	ValueConverters[Bool] = toBool
	ValueConverters[Enum] = toString
}

// NewReader creates a new reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{parser: NewParser(r)}
}

func (r *Reader) Read() (map[string]interface{}, error) {
	var row Row
	var err error

	if r.header == nil {
		r.header, err = r.readHeader()
		if err != nil {
			return nil, err
		}
		row = r.parser.Current()
	} else {
		row, err = r.parser.Read()
		if err != nil {
			return nil, err
		}
	}
	if bytes.HasPrefix(row[0], []byte("#close")) {
		return nil, io.EOF
	}
	record := make(map[string]interface{}, len(r.header.Fields))
	for i := 0; i < len(r.header.Fields); i++ {
		record[r.header.Fields[i]], err = r.readValue(row, i)
		if err != nil {
			return nil, err
		}
	}
	return record, nil
}

func (r *Reader) readHeader() (*Header, error) {
	header := Header{}
	for {
		row, err := r.parser.Read()
		if err != nil {
			return nil, err
		}
		r.parser.ResetRow()

		if !bytes.HasPrefix(row[0], []byte("#")) {
			break
		}
		if bytes.HasPrefix(row[0], []byte("#separator")) {
			parts := bytes.Split(row[0], []byte(" "))
			encodedSeparator := parts[1]
			sep, err := hex.DecodeString(string(encodedSeparator[2:]))
			if err != nil {
				return nil, fmt.Errorf("invalid separator")
			}
			header.Separator = sep[0]
			continue
		}
		switch string(row[0]) {
		case "#set_separator":
			header.SetSeparator = append(header.SetSeparator, row[1]...)
		case "#unset_field":
			header.Unset = append(header.Unset, row[1]...)
		case "#empty_field":
			header.Empty = append(header.Empty, row[1]...)
		case "#fields":
			for _, f := range row[1:] {
				header.Fields = append(header.Fields, string(f))
			}
		case "#types":
			for _, t := range row[1:] {
				fieldType, err := readFieldType(string(t))
				if err != nil {
					return nil, err
				}
				header.Types = append(header.Types, fieldType)
			}
		}
	}
	return &header, nil
}

func readFieldType(s string) (FieldType, error) {
	var container bool
	if strings.HasSuffix(s, "]") {
		start := strings.Index(s, "[")
		end := strings.Index(s[start:], "]")
		s = s[start+1 : start+end]
		container = true
	}
	if dataType, ok := dataTypeLookup[s]; ok {
		return FieldType{
			dataType:  dataType,
			container: container,
		}, nil
	}
	return FieldType{}, fmt.Errorf("unknown field type: %s", s)
}

func (r *Reader) readValue(row Row, idx int) (interface{}, error) {
	if bytes.Equal(row[idx], r.header.Unset) {
		return nil, nil
	}
	if bytes.Equal(row[idx], r.header.Empty) {
		if r.header.Types[idx].container {
			return []interface{}{}, nil
		}
		return nil, nil
	}
	converter := ValueConverters[r.header.Types[idx].dataType]
	if r.header.Types[idx].container {
		parts := bytes.Split(row[idx], r.header.SetSeparator)
		res := make([]interface{}, len(parts))
		for i := 0; i < len(parts); i++ {
			v, err := converter(parts[i])
			if err != nil {
				return nil, err
			}
			res[i] = v
		}
		return res, nil
	}
	return converter(row[idx])
}

func btos(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func asBytes(b []byte) (interface{}, error) {
	return b, nil
}

func toString(b []byte) (interface{}, error) {
	return string(b), nil
}

func toUint16(b []byte) (interface{}, error) {
	i, err := strconv.ParseUint(btos(b), 10, 16)
	return uint16(i), err
}

func toUint64(b []byte) (interface{}, error) {
	return strconv.ParseUint(btos(b), 10, 64)
}

func toFloat64(b []byte) (interface{}, error) {
	return strconv.ParseFloat(btos(b), 64)
}

func toBool(b []byte) (interface{}, error) {
	return bytes.Equal(b, []byte("T")), nil
}
