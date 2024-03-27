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
	scanner := bufio.NewScanner(r)
	scanner.Split(splitFunc)

	return &Parser{
		Delimiter: '\t',
		Copy:      false,
		scanner:   scanner,
	}
}

func splitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, data[0 : i+1], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

// dropCRLF drops a terminal (\r)\n from in and returns a (possibly shorter) out slice and whether dropping was done.
func dropCRLF(in []byte) (out []byte, dropped bool) {
	out = in

	if len(in) > 0 && in[len(in)-1] == '\n' {
		out = in[0 : len(in)-1]
		dropped = true
	}

	if len(out) > 0 && out[len(out)-1] == '\r' {
		out = out[0 : len(out)-1]
		dropped = true
	}

	return
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

	line, droppedCr := dropCRLF(line)
	if !droppedCr {
		return nil, ErrTruncatedLine
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
