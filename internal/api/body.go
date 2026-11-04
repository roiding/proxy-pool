package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"proxypool/internal/domain"
)

func readAll(r *http.Request) (string, error) {
	b, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeStrictJSON rejects unknown fields, trailing values, and oversized
// bodies. emptyOK is reserved for endpoints whose documented contract allows
// an entirely omitted body; an empty JSON object still decodes normally.
func decodeStrictJSON(w http.ResponseWriter, r *http.Request, limit int64,
	dst any, emptyOK bool) *domain.Error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		if emptyOK && err == io.EOF {
			return nil
		}
		return domain.InvalidRequest("invalid JSON body: %v", err)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return domain.InvalidRequest("JSON body must be an object")
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return domain.InvalidRequest("unexpected trailing data after JSON object")
	}
	strict := json.NewDecoder(bytes.NewReader(raw))
	strict.DisallowUnknownFields()
	if err := strict.Decode(dst); err != nil {
		return domain.InvalidRequest("invalid JSON body: %v", err)
	}
	return nil
}

// optionalStringSlice distinguishes the three JSON shapes a string-array
// field can take: a missing field, an explicit null, and an explicit array.
// It is intentionally a value rather than a pointer: json.Unmarshal skips
// custom unmarshalers and clears pointer fields for JSON null.
type optionalStringSlice struct {
	Present bool
	Null    bool
	Values  []string
}

func (f *optionalStringSlice) UnmarshalJSON(data []byte) error {
	f.Present = true
	if string(data) == "null" {
		f.Null = true
		return nil
	}
	if err := json.Unmarshal(data, &f.Values); err != nil {
		return fmt.Errorf("must be an array of strings or null: %v", err)
	}
	return nil
}

func (f optionalStringSlice) MarshalJSON() ([]byte, error) {
	if f.Null {
		return []byte("null"), nil
	}
	return json.Marshal(f.Values)
}

// optionalInt distinguishes an omitted integer from JSON null and a value.
// It is a value field for the same reason as optionalStringSlice.
type optionalInt struct {
	Present bool
	Null    bool
	Value   int
}

func (f *optionalInt) UnmarshalJSON(data []byte) error {
	f.Present = true
	if string(data) == "null" {
		f.Null = true
		return nil
	}
	n, err := strconv.Atoi(string(data))
	if err != nil {
		return fmt.Errorf("must be an integer")
	}
	f.Value = n
	return nil
}
