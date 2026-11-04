package scdn

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"proxypool/internal/domain"
)

func TestConfigValidateAndParams(t *testing.T) {
	ok := []Config{
		{Protocol: "http", FetchCount: 20},
		{Protocol: "https", CountryCode: "CN"},
		{Protocol: "socks5", FetchCount: 1},
		{Protocol: "http"},
	}
	for _, c := range ok {
		if err := c.Validate(); err != nil {
			t.Fatalf("config %+v should validate: %v", c, err)
		}
	}
	bad := []Config{
		{},
		{Protocol: "all"},
		{Protocol: "socks4"},
		{Protocol: "HTTP"},
		{Protocol: "http", FetchCount: 21},
		{Protocol: "http", CountryCode: "cn"},
		{Protocol: "http", CountryCode: "CHN"},
	}
	for _, c := range bad {
		if err := c.Validate(); err == nil {
			t.Fatalf("config %+v should be rejected", c)
		}
	}
	params := (Config{Protocol: "https", CountryCode: "US"}).QueryParams()
	if params.Get("protocol") != "https" || params.Get("count") != "20" || params.Get("country_code") != "US" {
		t.Fatalf("query params: %v", params)
	}
}

func vendorServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("protocol") == "" {
			t.Errorf("expected protocol query parameter")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestFetchParsesAndMapsEntries(t *testing.T) {
	srv := vendorServer(t, 200, `{"code":200,"message":"success","data":{"proxies":[
		"8.8.8.8:8080","8.8.4.4:3128","8.8.8.8:8080","9.9.9.9:1080", ""
	],"count":4}}`)
	defer srv.Close()
	res, err := Fetch(context.Background(), srv.Client(), srv.URL,
		Config{Protocol: "https", FetchCount: 5}, false)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if res.Requested != 5 || len(res.Entries) != 3 || len(res.Errors) != 1 {
		t.Fatalf("result: %+v", res)
	}
	if res.Entries[0].Transport != domain.TransportHTTP {
		t.Fatalf("https must map to the http transport: %+v", res.Entries[0])
	}
	if res.Entries[0].Host != "8.8.8.8" || res.Entries[0].Port != 8080 {
		t.Fatalf("entry: %+v", res.Entries[0])
	}
	if res.Errors[0].Line != 5 {
		t.Fatalf("empty row should be line 5: %+v", res.Errors[0])
	}
}

func TestFetchSocks5Mapping(t *testing.T) {
	srv := vendorServer(t, 200, `{"code":200,"message":"success","data":{"proxies":["9.9.9.9:1080"],"count":1}}`)
	defer srv.Close()
	res, err := Fetch(context.Background(), srv.Client(), srv.URL, Config{Protocol: "socks5"}, false)
	if err != nil || len(res.Entries) != 1 || res.Entries[0].Transport != domain.TransportSOCKS5 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestFetchRejectsBadRowsAndPrivateAddresses(t *testing.T) {
	srv := vendorServer(t, 200, `{"code":200,"message":"success","data":{"proxies":[
		"10.0.0.1:8080","127.0.0.1:8080","203.0.113.1","203.0.113.2:0","203.0.113.3:notaport",
		"2001:db8::1:8080","[2606:4700::5]:8080","203.0.113.4:70000","not_a_host!:8080"
	],"count":9}}`)
	defer srv.Close()
	res, err := Fetch(context.Background(), srv.Client(), srv.URL, Config{Protocol: "http"}, false)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(res.Entries) != 1 || res.Entries[0].Host != "2606:4700::5" || res.Entries[0].Port != 8080 {
		t.Fatalf("only bracketed IPv6 should survive: %+v errors=%+v", res.Entries, res.Errors)
	}
	if len(res.Errors) != 8 {
		t.Fatalf("expected 8 rejected rows, got %+v", res.Errors)
	}
	if !strings.Contains(res.Errors[0].Error, "address policy") ||
		!strings.Contains(res.Errors[1].Error, "address policy") {
		t.Fatalf("private addresses must be rejected: %+v", res.Errors)
	}
}

func TestFetchErrorEnvelopes(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		kind   ErrorKind
	}{
		{"provider code", 200, `{"code":500,"message":"boom","data":{}}`, KindProvider},
		{"bad json", 200, `{"code":200,`, KindFormat},
		{"http status", 503, `busy`, KindProvider},
		{"malformed data", 200, `{"code":200,"message":"success","data":{"proxies":"notanarray"}}`, KindFormat},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := vendorServer(t, tc.status, tc.body)
			defer srv.Close()
			_, err := Fetch(context.Background(), srv.Client(), srv.URL, Config{Protocol: "http"}, false)
			var serr *Error
			if err == nil || !asScdn(err, &serr) || serr.Kind != tc.kind {
				t.Fatalf("want %s error, got %v", tc.kind, err)
			}
		})
	}
}

func TestFetchBoundsResponseSize(t *testing.T) {
	srv := vendorServer(t, 200, `{"code":200,"message":"`+strings.Repeat("x", MaxResponseBytes)+`"}`)
	defer srv.Close()
	_, err := Fetch(context.Background(), srv.Client(), srv.URL, Config{Protocol: "http"}, false)
	var serr *Error
	if err == nil || !asScdn(err, &serr) || serr.Kind != KindFormat {
		t.Fatalf("oversized payload must fail as format error, got %v", err)
	}
}

func asScdn(err error, target **Error) bool {
	for err != nil {
		if e, ok := err.(*Error); ok {
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
