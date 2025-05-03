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

// validateKey checks if the key is not empty
func validateKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("key cannot be empty")
	}
	return nil
}

// validatePutArgs checks if both key and value are valid for put operation
func validatePutArgs(key, value string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("value cannot be empty for put operation")
	}
	return nil
}

func main() {
	addr := flag.String("addr", "localhost:9002", "leader address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("usage: client [get|put|del] key [value]")
		os.Exit(1)
	}

	op := args[0]
	key := args[1]
	var val string

	// Validate based on operation type
	switch op {
	case "put":
		if len(args) != 3 {
			fmt.Println("put requires value")
			os.Exit(1)
		}
		val = args[2]
		if err := validatePutArgs(key, val); err != nil {
			fmt.Printf("validation error: %v\n", err)
			os.Exit(1)
		}
	case "get", "del":
		if err := validateKey(key); err != nil {
			fmt.Printf("validation error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Printf("unknown operation: %s\n", op)
		os.Exit(1)
	}

	// Create request based on operation type
	url := fmt.Sprintf("http://%s/%s", *addr, op)
	var req *http.Request
	var err error

	switch op {
	case "get":
		// For GET operations, add key as URL parameter
		req, err = http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", url, key), nil)
	case "del":
		// For DEL operations, use DELETE method with key in URL
		req, err = http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%s", url, key), nil)
	case "put":
		// For PUT operations, use POST method with JSON body
		reqBody, err := json.Marshal(cmdArgs{Key: key, Value: val})
		if err != nil {
			fmt.Printf("failed to marshal request: %v\n", err)
			os.Exit(1)
		}
		req, err = http.NewRequest(http.MethodPost, url, bytes.NewReader(reqBody))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
		}
	}

	if err != nil {
		fmt.Printf("failed to create request: %v\n", err)
		os.Exit(1)
	}

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("failed to read response: %v\n", err)
		os.Exit(1)
	}

	// Handle different status codes
	switch resp.StatusCode {
	case http.StatusOK:
		fmt.Println(string(body))
	case http.StatusNotFound:
		fmt.Printf("key not found: %s\n", key)
	default:
		fmt.Printf("request failed with status %d: %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}
}