package usage

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestBarkRejectsFailedApplicationAcknowledgement(t *testing.T) {
	for _, body := range []string{`{"code":400,"message":"device-secret"}`, `<html>device-secret</html>`, `{}`} {
		client, err := NewBarkClient("https://bark.example.com/device-secret", time.Second, 0)
		if err != nil {
			t.Fatal(err)
		}
		client.client.Transport = huaweiFixtureTransport(func(*http.Request) (*http.Response, error) {
			return huaweiFixtureResponse(200, body), nil
		})
		if err := client.Push(context.Background(), "test", "test"); err == nil || strings.Contains(err.Error(), "device-secret") {
			t.Fatalf("false or unsafe push acknowledgement: %v", err)
		}
	}
	client, _ := NewBarkClient("https://bark.example.com/device-secret", time.Second, 0)
	client.client.Transport = huaweiFixtureTransport(func(*http.Request) (*http.Response, error) {
		return huaweiFixtureResponse(200, `{"code":200,"message":"success"}`), nil
	})
	if err := client.Push(context.Background(), "test", "test"); err != nil {
		t.Fatal(err)
	}
}
