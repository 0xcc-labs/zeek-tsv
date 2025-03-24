package tsv

import (
	"bytes"
	"io"
)

// LazyReader is a zeek tsv file reader that defers type conversions.
type LazyReader struct {
	parser       *Parser
	header       *Header
	fieldIndex   map[string]int
	recordOffset uint64
}

// NewLazyReader creates a new lazy reader.
func NewLazyReader(r io.Reader) *LazyReader {
	return &LazyReader{
		parser: NewParser(r),
	}
}

// NewLazySeekableReader creates a new lazy seekable reader.
func NewLazySeekableReader(r io.ReadSeeker) *LazyReader {
	return &LazyReader{
		parser: NewSeekableParser(r),
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

	return &LazyRecord{
		lazyReader: r,
		row:        row,
	}, nil
}

func (r *LazyReader) Offset() uint64 {
	return r.recordOffset
}

func (r *LazyReader) Seek(offset uint64) error {
	return r.parser.Seek(offset)
}
