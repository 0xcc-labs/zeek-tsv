package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"

	zeek "github.com/0xcc-labs/zeek-tsv"
)

func main() {
	reader := zeek.NewReader(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	encoder := json.NewEncoder(out)
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if err := encoder.Encode(record); err != nil {
			log.Fatal(err)
		}
	}
}
