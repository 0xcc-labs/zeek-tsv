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
	fh        io.ReadSeeker
	reader    *bufio.Reader
	row       Row
	n         int
	offset    uint64
}

// NewParser returns a new Parser that reads from r.
func NewParser(r io.Reader) *Parser {
	reader := bufio.NewReader(r)

	return &Parser{
		Delimiter: '\t',
		reader:    reader,
	}
}

// NewSeekableParser returns a new Parser that reads from r and supports seeking via Seek().
func NewSeekableParser(r io.ReadSeeker) *Parser {
	reader := bufio.NewReader(r)

	return &Parser{
		Delimiter: '\t',
		reader:    reader,
		fh:        r,
	}
}

// Read reads one Row from r.
func (p *Parser) Read() (Row, error) {
	line, err := p.reader.ReadBytes('\n')
	if err != nil {
		if err == io.EOF && len(line) != 0 && !bytes.HasPrefix(line, []byte("#")) {
			return nil, ErrTruncatedLine
		}
		// Remaining possibilities are:
		// - io.EOF with truncation on a line starting with '#' (typically a "#close ..." footer)
		// - io.EOF with no line truncation
		// - some other (non-EOF) error
		return nil, err
	}

	p.offset += uint64(len(line))

	if p.n == 0 {
		// count columns
		p.n = bytes.Count(line, []byte{p.Delimiter}) + 1
		p.row = make(Row, p.n)
	}

	var n, start int
	for i, c := range line {
		if c == p.Delimiter {
			p.row[n] = line[start:i]
			start = i + 1
			n++
		}
	}

	// Handle final column, including stripping (\r)\n from it.
	end := len(line) - 1
	if line[end] == '\n' {
		end--
	}
	if line[end] == '\r' {
		end--
	}
	p.row[n] = line[start : end+1]

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

func (p *Parser) Seek(offset uint64) error {
	if p.fh == nil {
		return ErrSeekingUnsupported
	}
	_, err := p.fh.Seek(int64(offset), 0)
	if err != nil {
		return err
	}

	p.reader.Reset(p.fh)

	return nil
}
