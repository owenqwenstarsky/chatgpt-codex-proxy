package server

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const (
	// These limits are deliberately generous enough for existing image/file
	// inputs while preventing a single request from consuming unbounded memory.
	maxRequestBodyBytes = 256 << 20
	maxDecodedBodyBytes = 256 << 20
)

func readRequestBody(req *http.Request) ([]byte, error) {
	body, err := readLimitedBody(req.Body, maxRequestBodyBytes)
	if err != nil {
		return nil, err
	}

	encodings := strings.Split(req.Header.Get("Content-Encoding"), ",")
	for i := len(encodings) - 1; i >= 0; i-- {
		encoding := strings.ToLower(strings.TrimSpace(encodings[i]))
		switch encoding {
		case "", "identity":
			continue
		case "zstd":
			decoder, err := zstd.NewReader(bytes.NewReader(body))
			if err != nil {
				return nil, fmt.Errorf("failed to create zstd request decoder: %w", err)
			}
			body, err = readLimitedBody(decoder, maxDecodedBodyBytes)
			decoder.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to decode zstd request body: %w", err)
			}
		default:
			return nil, fmt.Errorf("unsupported request content encoding: %s", encoding)
		}
	}

	return body, nil
}

func readLimitedBody(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("request body exceeds %d bytes", limit)
	}
	return body, nil
}
