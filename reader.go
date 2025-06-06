package tsv

import (
	"bytes"
	"encoding/hex"
	"io"
	"strconv"
	"strings"
	"unsafe"
)

// Record is a tsv file record.
type Record map[string]interface{}

// KeyTransform is a key transform function.
type KeyTransform func(key string) string

// Reader is a zeek tsv file reader.
type Reader struct {
	parser       *Parser
	header       *Header
	keyTransform KeyTransform
	omitEmpty    bool
	recordOffset uint64
}

// Header is a zeek tsv file header.
type Header struct {
	Separator    byte
	Fields       []string
	Types        []FieldType
	Unset        []byte
	Empty        []byte
	SetSeparator []byte
	Path         string
	Length       uint64
}

// FieldType is a zeek field type.
type FieldType struct {
	Type        DataType
	IsContainer bool
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
	Subnet
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
	"subnet":   Subnet,
}

// ValueConverters maps DataTypes to converter functions.
var ValueConverters [11]func(b []byte) (interface{}, error)

func init() {
	ValueConverters[String] = ToString
	ValueConverters[Time] = ToFloat64
	ValueConverters[Addr] = ToString
	ValueConverters[Port] = ToUint16
	ValueConverters[Int] = ToInt64
	ValueConverters[Double] = ToFloat64
	ValueConverters[Count] = ToUint64
	ValueConverters[Interval] = ToFloat64
	ValueConverters[Bool] = ToBool
	ValueConverters[Enum] = ToString
	ValueConverters[Subnet] = ToString
}

// NewReader creates a new reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{parser: NewParser(r)}
}

// NewSeekableReader creates a new seekable reader.
func NewSeekableReader(r io.ReadSeeker) *Reader {
	return &Reader{
		parser: NewSeekableParser(r),
	}
}

// WithKeyTransform configures the reader to transform record keys.
func (r *Reader) WithKeyTransform(xform KeyTransform) *Reader {
	r.keyTransform = xform
	return r
}

// OmitEmpty configures the reader to omit empty fields from returned records.
func (r *Reader) OmitEmpty(b bool) *Reader {
	r.omitEmpty = b
	return r
}

// Header returns the log meta-info.
func (r *Reader) Header() *Header {
	return r.header
}

func (r *Reader) Read() (Record, error) {
	var row Row
	var err error

	if r.header == nil {
		r.header, err = readHeader(r.parser, r.keyTransform)
		if err != nil {
			return nil, err
		}

		r.recordOffset = r.header.Length
		row = r.parser.Current()
	} else {
		r.recordOffset = r.parser.offset
		row, err = r.parser.Read()
		if err != nil {
			return nil, err
		}
	}
	if bytes.HasPrefix(row[0], []byte("#close")) {
		return nil, io.EOF
	}
	record := make(Record, len(r.header.Fields))
	for i := 0; i < len(r.header.Fields); i++ {
		v, err := r.readValue(row, i)
		if err != nil {
			return nil, err
		}
		if !r.omitEmpty || v != nil {
			record[r.header.Fields[i]] = v
		}
	}
	return record, nil
}

func (r *Reader) Offset() uint64 {
	return r.recordOffset
}

func (r *Reader) Seek(offset uint64) error {
	return r.parser.Seek(offset)
}

func readHeader(parser *Parser, keyTransform KeyTransform) (*Header, error) {
	header := Header{}
	for {
		header.Length = parser.offset
		row, err := parser.Read()
		if err != nil {
			return nil, err
		}
		parser.ResetRow()

		if !bytes.HasPrefix(row[0], []byte("#")) {
			break
		}
		if bytes.HasPrefix(row[0], []byte("#separator")) {
			parts := bytes.Split(row[0], []byte(" "))
			encodedSeparator := parts[1]
			sep, err := hex.DecodeString(string(encodedSeparator[2:]))
			if err != nil {
				return nil, ErrInvalidSeparator
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
				field := string(f)
				if keyTransform != nil {
					field = keyTransform(field)
				}
				header.Fields = append(header.Fields, field)
			}
		case "#types":
			for _, t := range row[1:] {
				fieldType, err := readFieldType(string(t))
				if err != nil {
					return nil, err
				}
				header.Types = append(header.Types, fieldType)
			}
		case "#path":
			header.Path = string(row[1][:])
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
			Type:        dataType,
			IsContainer: container,
		}, nil
	}
	return FieldType{}, ErrorInvalidFieldType{TypeName: s}
}

func (r *Reader) readValue(row Row, idx int) (interface{}, error) {
	if idx >= len(row) {
		return nil, ErrTruncatedLine
	}
	if bytes.Equal(row[idx], r.header.Unset) {
		return nil, nil
	}
	if bytes.Equal(row[idx], r.header.Empty) {
		if r.header.Types[idx].IsContainer {
			return nil, nil
		}
		return nil, nil
	}
	converter := ValueConverters[r.header.Types[idx].Type]
	if r.header.Types[idx].IsContainer {
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

// AsBytes converter returns input bytes untouched.
func AsBytes(b []byte) (interface{}, error) {
	return b, nil
}

// ToString converter converts input to string.
func ToString(b []byte) (interface{}, error) {
	return string(b), nil
}

// ToUint16 converter converts input to uint16.
func ToUint16(b []byte) (interface{}, error) {
	i, err := strconv.ParseUint(btos(b), 10, 16)
	return uint16(i), err
}

// ToInt64 converter converts input to int64.
func ToInt64(b []byte) (interface{}, error) {
	return strconv.ParseInt(btos(b), 10, 64)
}

// ToUint64 converter converts input to uint64.
func ToUint64(b []byte) (interface{}, error) {
	return strconv.ParseUint(btos(b), 10, 64)
}

// ToFloat64 converter converts input to float64.
func ToFloat64(b []byte) (interface{}, error) {
	return strconv.ParseFloat(btos(b), 64)
}

// ToBool converter converts input to bool.
func ToBool(b []byte) (interface{}, error) {
	return bytes.Equal(b, []byte("T")), nil
}
