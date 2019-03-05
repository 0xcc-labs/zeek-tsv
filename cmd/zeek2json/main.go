package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"strings"

	"github.com/francoispqt/gojay"

	zeek "github.com/0xcc-labs/zeek-tsv"
)

func main() {
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	reader := zeek.NewReader(os.Stdin).WithKeyTransform(xformKey)
	encoder := gojay.NewEncoder(out)
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if err := encoder.Encode(jsonRecord(record)); err != nil {
			log.Fatal(err)
		}
		out.WriteByte('\n')
	}
}

func xformKey(key string) string {
	return strings.ReplaceAll(key, ".", "_")
}

type jsonRecord zeek.Record

func (r jsonRecord) MarshalJSONObject(enc *gojay.Encoder) {
	for k, v := range r {
		if v, ok := v.([]interface{}); ok {
			enc.AddInterfaceKey(k, jsonArray(v))
			continue
		}
		enc.AddInterfaceKey(k, v)
	}
}

func (r jsonRecord) IsNil() bool {
	return r == nil
}

type jsonArray []interface{}

func (a jsonArray) MarshalJSONArray(enc *gojay.Encoder) {
	for _, v := range a {
		enc.AddInterface(v)
	}
}

func (a jsonArray) IsNil() bool {
	return len(a) == 0
}
