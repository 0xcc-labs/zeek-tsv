package tsv

import (
	"errors"
	"fmt"
)

var ErrTruncatedLine = errors.New("truncated line")
var ErrInvalidSeparator = errors.New("invalid separator")

type ErrorInvalidFieldType struct {
	TypeName string
}

func (e ErrorInvalidFieldType) Error() string {
	return fmt.Sprintf("unknown field type: %s", e.TypeName)
}
