package tsv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

var input = `#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	test
#open	2019-01-01-00-00-00
#fields	ts	uid	id.orig_h	id.orig_p	proto	duration	bytes	num	orig	domains	durations
#types	time	string	addr	port	enum	interval	count	int	bool	vector[string]	vector[interval]
1546304400.000001	CCb2Mx28qOMGD3hxab	1.1.1.1	80	udp	3.755453	1001	-10	T	a.com,b.com	1,23.45
-	-	-	-	-	-	-	-	-	-	-
(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)
#close	2019-01-01-00-00-01
`

var inputHeaderLength = uint64(279)

var truncatedInput1 = `#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	test
#open	2019-01-01-00-00-00
#fields	ts	uid	id.orig_h	id.orig_p	proto	duration	bytes	num	orig	domains	durations
#types	time	string	addr	port	enum	interval	count	int	bool	vector[string]	vector[interval]
1546304400.000001	CCb2Mx28qOMGD3hxab	1.1.1.1	80	udp	3.755453	1001	-10	T	a.com,b.com	1,23.45
1546304400.000002	CCb2Mx28qOMGD3hxab	1.1.1.1	80	udp	3.755453`

var truncatedInput2 = `#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	test
#open	2019-01-01-00-00-00
#fields	ts	uid	id.orig_h	id.orig_p	proto	duration	bytes	num	orig	domains	durations
#types	time	string	addr	port	enum	interval	count	int	bool	vector[string]	vector[interval]
1546304400.000001	CCb2Mx28qOMGD3hxab	1.1.1.1	80	udp	3.755453	1001	-10	T	a.com,b.com	1,23.45
1546304400.000001	CCb2Mx28qOMGD3hxab	1.1.1.1	80	udp	3.755453	1001	-10	T	a.com,b.com	1,23.4`

var truncatedInput3 = `#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	test
#open	2019-01-01-00-00-00
#fields	ts	uid	id.orig_h	id.orig_p	proto	duration	bytes	num	orig	domains	durations
#types	time	string	addr	port	enum	interval	count	int	bool	vector[string]	vector[interval]
1546304400.000001	CCb2Mx28qOMGD3hxab	1.1.1.1	80	udp	3.755453	1001	-10	T	a.com,b.com	1,23.45
-	-	-	-	-	-	-	-	-	-	-
(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)
#close	2019-01-01-00-00-01`

const giantColumnSize = 128 * 1024

var giantInput = `#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	test
#open	2019-01-01-00-00-00
#fields	ts	foo	bar
#types	time	string	string
1546304400.000001	` + strings.Repeat("a", giantColumnSize) + `	` + strings.Repeat("a", giantColumnSize) + `
#close	2019-01-01-00-00-01
`

func TestReadHeader(t *testing.T) {
	reader := NewReader(strings.NewReader(input))
	header, err := readHeader(reader.parser, reader.keyTransform)
	if err != nil {
		t.Fatal(err)
	}
	if header.Separator != '\t' {
		t.Error("invalid separator")
	}
	if !reflect.DeepEqual(header.Fields, []string{"ts", "uid", "id.orig_h", "id.orig_p", "proto", "duration", "bytes", "num", "orig", "domains", "durations"}) {
		t.Error("invalid fields")
	}
	if !bytes.Equal(header.Unset, []byte("-")) {
		t.Error("invalid unset value")
	}
	if !bytes.Equal(header.Empty, []byte("(empty)")) {
		t.Error("invalid empty value")
	}
	if !bytes.Equal(header.SetSeparator, []byte(",")) {
		t.Error("invalid set separator")
	}
}

var expected = []Record{
	{
		"ts":        float64(1546304400.000001),
		"uid":       "CCb2Mx28qOMGD3hxab",
		"id.orig_h": "1.1.1.1",
		"id.orig_p": uint16(80),
		"proto":     "udp",
		"duration":  3.755453,
		"bytes":     uint64(1001),
		"num":       int64(-10),
		"orig":      true,
		"domains":   []interface{}{"a.com", "b.com"},
		"durations": []interface{}{float64(1), float64(23.45)},
	},
	Record{},
	Record{},
}

var expectedColumns = []string{
	"ts",
	"uid",
	"id.orig_h",
	"id.orig_p",
	"proto",
	"duration",
	"bytes",
	"num",
	"orig",
	"domains",
	"durations",
}

var expectedStrings = [][]string{
	{"1546304400.000001",
		"CCb2Mx28qOMGD3hxab",
		"1.1.1.1",
		"80",
		"udp",
		"3.755453",
		"1001",
		"-10",
		"T",
		"a.com,b.com",
		"1,23.45"},
	{"-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"},
	{"(empty)", "(empty)", "(empty)", "(empty)", "(empty)", "(empty)", "(empty)", "(empty)", "(empty)",
		"(empty)", "(empty)"},
}

var expectedGiant = Record{
	"ts":  float64(1546304400.000001),
	"foo": strings.Repeat("a", giantColumnSize),
}

func MakeReadTester(input string, expectedOutput []Record, expectedError error) func(t *testing.T) {
	return func(t *testing.T) {
		reader := NewReader(strings.NewReader(input))
		actual, actualError := collectWithError(reader)
		if len(expectedOutput) != len(actual) {
			t.Errorf("expected %d records, got %d", len(expectedOutput), len(actual))
		} else {
			for i := 0; i < len(expectedOutput); i++ {
				for k, v := range expectedOutput[i] {
					if !reflect.DeepEqual(v, actual[i][k]) {
						t.Errorf("%s mismatch. expected %v (%T), got %v (%T)",
							k, v, v, actual[i][k], actual[i][k])
					}
				}
			}
		}

		if !reflect.DeepEqual(expectedError, actualError) {
			t.Errorf("expected error %v (%T), got %v (%T)",
				expectedError, expectedError, actualError, actualError)
		}
	}
}

func MakeLazyReadTester(input string, expectedOutput []Record, expectedError error) func(t *testing.T) {
	return func(t *testing.T) {
		lazyReader := NewLazyReader(strings.NewReader(input))
		actual, actualError := collectLazilyWithError(lazyReader)
		if len(expectedOutput) != len(actual) {
			t.Errorf("expected %d records, got %d", len(expectedOutput), len(actual))
		} else {
			for i := 0; i < len(expectedOutput); i++ {
				for k, v := range expectedOutput[i] {
					if !reflect.DeepEqual(v, actual[i][k]) {
						t.Errorf("%s mismatch. expected %v (%T), got %v (%T)",
							k, v, v, actual[i][k], actual[i][k])
					}
				}
			}
		}

		if !reflect.DeepEqual(expectedError, actualError) {
			t.Errorf("expected error %v (%T), got %v (%T)",
				expectedError, expectedError, actualError, actualError)
		}

		lazyReader = NewLazyReader(strings.NewReader(input))
		actualBytes, actualError := collectBytesLazilyWithError(lazyReader)

		if len(expectedOutput) != len(actualBytes) {
			t.Errorf("(bytes) expected %d records, got %d", len(expectedOutput), len(actualBytes))
		} else {
			for i := 0; i < len(actualBytes); i++ {
				for idx, v := range expectedStrings[i] {

					if v == "-" || v == "(empty)" {
						v = ""
					}

					if v != string(actualBytes[i][idx]) {
						t.Errorf("%d/%d (bytes)  %s mismatch. expected \"%s\" (%T), got \"%s\" (%T)",
							i, idx, expectedColumns[idx],
							v, v,
							string(actualBytes[i][idx]), string(actualBytes[i][idx]))
					}
				}
			}
		}

		if !reflect.DeepEqual(expectedError, actualError) {
			t.Errorf("(bytes) expected error %v (%T), got %v (%T)",
				expectedError, expectedError, actualError, actualError)
		}
	}
}

func TestHeaderLength(t *testing.T) {
	reader := NewReader(strings.NewReader(input))
	if _, err := reader.Read(); err != nil {
		t.Fatal(err)
	}

	if reader.Header().Length != inputHeaderLength {
		t.Fatalf("Got header length of %d (wanted %d)", reader.Header().Length, inputHeaderLength)
	}
}

func TestLazySeekableReader(t *testing.T) {
	reader := NewLazySeekableReader(strings.NewReader(input))

	// Read row 0
	row, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}

	// Now we know the offset of the first row should be == the header length
	if reader.Offset() != inputHeaderLength {
		t.Fatalf("Got offset of of %d (wanted %d)", reader.Offset(), inputHeaderLength)
	}

	// Get the byte length of row 0
	var rowLen uint64
	for _, val := range row.Row() {
		rowLen += uint64(len(val) + 1) // + 1 for each \t and the final \n
	}

	// Read row 1
	if _, err := reader.Read(); err != nil {
		t.Fatal(err)
	}

	// Now offset of row 1 should be += length of row 0
	if reader.Offset() != inputHeaderLength+rowLen {
		t.Fatalf("Got offset of of %d (wanted %d)", reader.Offset(), inputHeaderLength+rowLen)
	}

	// Seek back to row 0
	if err := reader.Seek(inputHeaderLength); err != nil {
		t.Fatal(err)
	}

	// Re-read row 0
	row0Again, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}

	for i, valA := range row.Row() {
		valB := row0Again.Row()[i]
		if !bytes.Equal(valA, valB) {
			t.Errorf("Row 0 col %d not equal on re-read: %s vs %s", i, string(valA), string(valB))
		}
	}
}

func TestRead(t *testing.T) {
	t.Run("all ok", MakeReadTester(input, expected, io.EOF))
	t.Run("line truncated in the middle (on a delimiter)",
		MakeReadTester(truncatedInput1, []Record{expected[0]}, ErrTruncatedLine))
	t.Run("line truncated inside the last column",
		MakeReadTester(truncatedInput2, []Record{expected[0]}, ErrTruncatedLine))
	t.Run("#close footer line truncated",
		MakeReadTester(truncatedInput3, expected, io.EOF))
	t.Run(fmt.Sprintf("line with %d byte column", giantColumnSize),
		MakeReadTester(giantInput, []Record{expectedGiant}, io.EOF))
}

func TestLazyRead(t *testing.T) {
	t.Run("all ok", MakeLazyReadTester(input, expected, io.EOF))
	t.Run("line truncated in the middle (on a delimiter)",
		MakeLazyReadTester(truncatedInput1, []Record{expected[0]}, ErrTruncatedLine))
	t.Run("line truncated inside the last column",
		MakeLazyReadTester(truncatedInput2, []Record{expected[0]}, ErrTruncatedLine))
	//t.Run(fmt.Sprintf("line with %d byte column", giantColumnSize),
	//	MakeLazyReadTester(giantInput, []Record{expectedGiant}, io.EOF))
}

func TestReadFieldType(t *testing.T) {
	var tests = []struct {
		in  string
		out FieldType
	}{
		{
			in: "bool",
			out: FieldType{
				Type:        Bool,
				IsContainer: false,
			},
		},
		{
			in: "vector[string]",
			out: FieldType{
				Type:        String,
				IsContainer: true,
			},
		},
		{
			in: "vector[interval]",
			out: FieldType{
				Type:        Interval,
				IsContainer: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			f, err := readFieldType(tt.in)
			if err != nil {
				t.Error(err)
				return
			}
			if f != tt.out {
				t.Errorf("got %v, want %v", f, tt.out)
			}
		})
	}
}

func TestTransformKeys(t *testing.T) {
	xform := func(key string) string {
		return strings.ReplaceAll(key, ".", "_")
	}
	reader := NewReader(strings.NewReader(input)).WithKeyTransform(xform)
	record, _ := reader.Read()
	if _, ok := record["id_orig_h"]; !ok {
		t.Errorf("expected transformed key")
	}
	if len(expected[0]) != len(record) {
		t.Errorf("expected record to have %v fields, got %v", len(expected[0]), len(record))
	}
}

func TestOmitEmpty(t *testing.T) {
	reader := NewReader(strings.NewReader(input)).OmitEmpty(true)

	records := collect(reader)

	if len(expected[0]) != len(records[0]) {
		t.Errorf("expected record to have %v fields, got %v", len(expected[0]), len(records[0]))
	}
	for i := 1; i < len(records); i++ {
		if len(records[i]) != 0 {
			t.Errorf("expected empty record, got %v fields", len(records[i]))
		}
	}
}

func collect(reader *Reader) (records []Record) {
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		records = append(records, record)
	}
	return
}

func collectWithError(reader *Reader) (records []Record, err error) {
	for {
		var record Record
		record, err = reader.Read()
		if err != nil {
			break
		}
		records = append(records, record)
	}
	return
}

func collectLazilyWithError(lazyReader *LazyReader) (records []Record, err error) {
	for {
		record := make(Record)
		var lazyRecord *LazyRecord
		lazyRecord, err = lazyReader.Read()
		if err != nil {
			break
		}

		for _, field := range lazyReader.Header().Fields {
			var value interface{}
			value, err = lazyRecord.ValueByName(field)
			if err != nil {
				return
			}

			record[field] = value
		}

		records = append(records, record)
	}
	return
}

func collectBytesLazilyWithError(lazyReader *LazyReader) (rows [][][]byte, err error) {
	for {
		var lazyRecord *LazyRecord
		lazyRecord, err = lazyReader.Read()
		if err != nil {
			break
		}

		var row [][]byte
		for i, field := range lazyReader.Header().Fields {
			var buf []byte
			buf, err = lazyRecord.BytesByName(field)
			if err != nil {
				return
			}

			var cmpBytes []byte
			cmpBytes, err = lazyRecord.BytesByIndex(i)
			if err != nil {
				return
			}
			if bytes.Compare(buf, cmpBytes) != 0 {
				err = errors.New("BytesByName() and BytesByIndex for the same column differ")
			}

			row = append(row, buf)
		}

		rows = append(rows, row)
	}
	return
}
