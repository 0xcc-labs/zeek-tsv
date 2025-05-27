package tsv

import (
	"bytes"
	"fmt"
)

type ErrNoSuchField struct {
	field string
}

func (e ErrNoSuchField) Error() string {
	return fmt.Sprintf("no such field: %s", e.field)
}

type LazyRecord struct {
	lazyReader *LazyReader
	row        Row
}

func (r *LazyRecord) BytesByName(field string) ([]byte, error) {
	idx, ok := r.lazyReader.fieldIndex[field]
	if !ok {
		return nil, ErrNoSuchField{field: field}
	}

	return r.BytesByIndex(idx)
}

func (r *LazyRecord) BytesByIndex(idx int) ([]byte, error) {
	if idx >= len(r.row) {
		return nil, ErrTruncatedLine
	}
	if bytes.Equal(r.row[idx], r.lazyReader.header.Unset) {
		return nil, nil
	}
	if bytes.Equal(r.row[idx], r.lazyReader.header.Empty) {
		if r.lazyReader.header.Types[idx].IsContainer {
			return nil, nil
		}
		return nil, nil
	}

	return r.row[idx], nil
}

func (r *LazyRecord) ValueByName(field string) (interface{}, error) {
	idx, ok := r.lazyReader.fieldIndex[field]
	if !ok {
		return nil, ErrNoSuchField{field: field}
	}

	return r.ValueByIndex(idx)
}

func (r *LazyRecord) ValueByIndex(idx int) (interface{}, error) {
	buf, err := r.BytesByIndex(idx)
	if err != nil {
		return nil, err
	}
	if buf == nil {
		return nil, nil
	}

	converter := ValueConverters[r.lazyReader.header.Types[idx].Type]
	if r.lazyReader.header.Types[idx].IsContainer {
		parts := bytes.Split(buf, r.lazyReader.header.SetSeparator)
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
	return converter(buf)
}
