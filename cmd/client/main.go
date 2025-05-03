package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type cmdArgs struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func validateInput(key, value string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("value cannot be empty")
	}
	return nil
}

func main() {
	addr := flag.String("addr", "localhost:9002", "leader address")
	flag.Parse()

	if flag.NArg() < 2 {
		fmt.Println("usage: client [get|put|del] key [value]")
		os.Exit(1)
	}

	op := flag.Arg(0)
	key := flag.Arg(1)
	var val string

	if op == "put" {
		if flag.NArg() != 3 {
			fmt.Println("put requires value")
			os.Exit(1)
		}
		val = flag.Arg(2)
		if err := validateInput(key, val); err != nil {
			fmt.Printf("validation error: %v\n", err)
			os.Exit(1)
		}
	} else if op == "get" || op == "del" {
		if err := validateInput(key, "dummy"); err != nil {
			fmt.Printf("validation error: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("unknown operation: %s\n", op)
		os.Exit(1)
	}

	reqBody, err := json.Marshal(cmdArgs{Key: key, Value: val})
	if err != nil {
		fmt.Printf("failed to marshal request: %v\n", err)
		os.Exit(1)
	}

	url := fmt.Sprintf("http://%s/%s", *addr, op)
	resp, err := http.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		fmt.Printf("request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("request failed with status %d: %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("failed to read response: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(body))
}
