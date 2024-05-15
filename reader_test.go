package tsv

import (
	"bufio"
	"bytes"
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
	header, err := reader.readHeader()
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

var expectedGiant = Record{
	"ts":  float64(1546304400.000001),
	"foo": strings.Repeat("a", giantColumnSize),
}

func MakeReadTester(input string, expectedOutput []Record, expectedError error, bufSize *int) func(t *testing.T) {
	return func(t *testing.T) {
		reader := NewReader(strings.NewReader(input))
		if bufSize != nil {
			reader = reader.WithBufferSize(*bufSize)
		}
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

func TestRead(t *testing.T) {
	t.Run("all ok", MakeReadTester(input, expected, io.EOF, nil))
	t.Run("line truncated in the middle (on a delimiter)",
		MakeReadTester(truncatedInput1, []Record{expected[0]}, ErrTruncatedLine, nil))
	t.Run("line truncated inside the last column",
		MakeReadTester(truncatedInput2, []Record{expected[0]}, ErrTruncatedLine, nil))
	t.Run(fmt.Sprintf("line with %d byte column and default buffer", giantColumnSize),
		MakeReadTester(giantInput, []Record{}, bufio.ErrTooLong, nil))
	bufSize := 1024 * 1024 * 1024
	t.Run(fmt.Sprintf("line with %d byte column and %d byte buffer", giantColumnSize, bufSize),
		MakeReadTester(giantInput, []Record{expectedGiant}, io.EOF, &bufSize))
}

func TestReadFieldType(t *testing.T) {
	var tests = []struct {
		in  string
		out FieldType
	}{
		{
			in: "bool",
			out: FieldType{
				dataType:  Bool,
				container: false,
			},
		},
		{
			in: "vector[string]",
			out: FieldType{
				dataType:  String,
				container: true,
			},
		},
		{
			in: "vector[interval]",
			out: FieldType{
				dataType:  Interval,
				container: true,
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
