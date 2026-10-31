package api

import (
	"io"
	"net/http"
)

func readAll(r *http.Request) (string, error) {
	b, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return "", err
	}
	return string(b), nil
}
