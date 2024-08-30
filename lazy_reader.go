package tsv

import (
	"bytes"
	"io"
)

// LazyReader is a zeek tsv file reader that defers type conversions.
type LazyReader struct {
	parser     *Parser
	header     *Header
	fieldIndex map[string]int
}

// NewLazyReader creates a new lazy reader.
func NewLazyReader(r io.Reader) *LazyReader {
	return &LazyReader{
		parser: NewParser(r),
	}
}

// Header returns the log meta-info.
func (r *LazyReader) Header() *Header {
	return r.header
}

func (r *LazyReader) Read() (*LazyRecord, error) {
	var row Row
	var err error

	if r.header == nil {
		r.header, err = readHeader(r.parser, nil)
		if err != nil {
			return nil, err
		}

		r.fieldIndex = make(map[string]int, len(r.header.Fields))
		for idx, fieldName := range r.header.Fields {
			r.fieldIndex[fieldName] = idx
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

	return &LazyRecord{
		lazyReader: r,
		row:        row,
	}, nil
}
