package tsv

import (
	"errors"
	"fmt"
)

var ErrTruncatedLine = errors.New("truncated line")
var ErrInvalidSeparator = errors.New("invalid separator")
var ErrSeekingUnsupported = errors.New("parser doesn't support seeking")

type ErrorInvalidFieldType struct {
	TypeName string
}

func (e ErrorInvalidFieldType) Error() string {
	return fmt.Sprintf("unknown field type: %s", e.TypeName)
}
