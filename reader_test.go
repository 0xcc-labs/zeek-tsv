package tsv

import (
	"bytes"
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
#fields	ts	uid	id.orig_h	id.orig_p	proto	duration	bytes	orig	domains	durations
#types	time	string	addr	port	enum	interval	count	bool	vector[string]	vector[interval]
1546304400.000001	CCb2Mx28qOMGD3hxab	1.1.1.1	80	udp	3.755453	1001	T	a.com,b.com	1,23.45
-	-	-	-	-	-	-	-	-	-
(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)	(empty)
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
	if !reflect.DeepEqual(header.Fields, []string{"ts", "uid", "id.orig_h", "id.orig_p", "proto", "duration", "bytes", "orig", "domains", "durations"}) {
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

func TestRead(t *testing.T) {
	expected := []Record{
		{
			"ts":        float64(1546304400.000001),
			"uid":       "CCb2Mx28qOMGD3hxab",
			"id.orig_h": "1.1.1.1",
			"id.orig_p": uint16(80),
			"proto":     "udp",
			"duration":  3.755453,
			"bytes":     uint64(1001),
			"orig":      true,
			"domains":   []interface{}{"a.com", "b.com"},
			"durations": []interface{}{float64(1), float64(23.45)},
		}, {
			"ts":        nil,
			"uid":       nil,
			"id.orig_h": nil,
			"id.orig_p": nil,
			"proto":     nil,
			"duration":  nil,
			"bytes":     nil,
			"orig":      nil,
			"domains":   nil,
			"durations": nil,
		}, {
			"ts":        nil,
			"uid":       nil,
			"id.orig_h": nil,
			"id.orig_p": nil,
			"proto":     nil,
			"duration":  nil,
			"bytes":     nil,
			"orig":      nil,
			"domains":   []interface{}{},
			"durations": []interface{}{},
		},
	}
	var actual []Record

	reader := NewReader(strings.NewReader(input))
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		actual = append(actual, record)
	}
	if len(expected) != len(actual) {
		t.Fatalf("expected %d records, got %d", len(expected), len(actual))
	}
	for i := 0; i < len(expected); i++ {
		for k, v := range expected[i] {
			if !reflect.DeepEqual(v, actual[i][k]) {
				t.Errorf("%s mismatch. expected %v (%T), got %v (%T)",
					k, v, v, actual[i][k], actual[i][k])
			}
		}
	}
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
	if 10 != len(record) {
		t.Errorf("expected record to have %v fields, got %v", 10, len(record))
	}
}
