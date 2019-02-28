package tsv

import (
	"bufio"
	"bytes"
	"io"
)

// Row is returned from Read.
type Row [][]byte

// Parser reads Rows from byte-separated input.
type Parser struct {
	Delimiter byte
	Copy      bool
	scanner   *bufio.Scanner
	row       Row
	n         int
}

// NewParser returns a new Parser that reads from r.
func NewParser(r io.Reader) *Parser {
	return &Parser{
		Delimiter: '\t',
		Copy:      false,
		scanner:   bufio.NewScanner(r),
	}
}

// Read reads one Row from r.
func (p *Parser) Read() (Row, error) {
	if !p.scanner.Scan() {
		if err := p.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	if p.n == 0 {
		// count columns
		p.n = bytes.Count(p.scanner.Bytes(), []byte{p.Delimiter}) + 1
		if p.Copy == false {
			p.row = make(Row, p.n)
		}
	}

	var line []byte

	if p.Copy {
		b := p.scanner.Bytes()
		line := make([]byte, len(b))
		copy(line, b)
		p.row = make(Row, p.n)
	} else {
		line = p.scanner.Bytes()
	}

	var n, start int
	for i, c := range line {
		if c == p.Delimiter {
			p.row[n] = line[start:i]
			start = i + 1
			n++
		}
	}
	p.row[n] = line[start:]

	return p.row, nil
}

// Current returns the most recently read Row.
func (p *Parser) Current() Row {
	return p.row
}

// ResetRow clears the row metadata.
func (p *Parser) ResetRow() {
	p.n = 0
}
