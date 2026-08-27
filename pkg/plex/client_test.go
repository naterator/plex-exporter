package plex

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ttPlex "github.com/timothystewart6/go-plex-client"
)

type simpleRT struct {
	status int
	body   string
}

func (s *simpleRT) RoundTrip(req *http.Request) (*http.Response, error) {
	resp := &http.Response{
		StatusCode: s.status,
		Status:     fmt.Sprintf("%d %s", s.status, http.StatusText(s.status)),
		Body:       io.NopCloser(bytes.NewBufferString(s.body)),
		Header:     make(http.Header),
		Request:    req,
	}
	resp.Header.Set("Content-Type", "application/json")
	return resp, nil
}

type clientRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f clientRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type trackingBody struct {
	reader    *strings.Reader
	fullyRead bool
	closed    bool
}

func (b *trackingBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if errors.Is(err, io.EOF) {
		b.fullyRead = true
	}
	return n, err
}

func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

func TestNewClientValidatesServerURL(t *testing.T) {
	for _, serverURL := range []string{"", "localhost:32400", "ftp://example.com", "http://"} {
		t.Run(serverURL, func(t *testing.T) {
			if _, err := NewClient(serverURL, "token"); err == nil {
				t.Fatalf("NewClient(%q) succeeded, want an invalid URL error", serverURL)
			}
		})
	}
}

func TestNewClientTimeoutAndTLSConfiguration(t *testing.T) {
	t.Run("valid timeout", func(t *testing.T) {
		t.Setenv("PLEX_CLIENT_TIMEOUT_SECONDS", "37")
		client, err := NewClient("http://example.com", "token")
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		if client.httpClient.Timeout != 37*time.Second {
			t.Fatalf("timeout = %v, want 37s", client.httpClient.Timeout)
		}
	})

	t.Run("overflow uses default", func(t *testing.T) {
		t.Setenv("PLEX_CLIENT_TIMEOUT_SECONDS", "9223372036854775807")
		client, err := NewClient("http://example.com", "token")
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		if client.httpClient.Timeout != 10*time.Second {
			t.Fatalf("timeout = %v, want default 10s", client.httpClient.Timeout)
		}
	})

	t.Run("case insensitive skip TLS", func(t *testing.T) {
		t.Setenv("SKIP_TLS_VERIFICATION", "TRUE")
		client, err := NewClient("https://example.com", "token")
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		transport, ok := client.httpClient.Transport.(*http.Transport)
		if !ok || transport.TLSClientConfig == nil || !transport.TLSClientConfig.InsecureSkipVerify {
			t.Fatal("expected an HTTP transport with TLS verification disabled")
		}
	})
}

func TestNewRequestHeaders(t *testing.T) {
	c, err := NewClient("http://example.com", "fake-token-123")
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	req, err := c.NewRequest("GET", "/path")
	if err != nil {
		t.Fatalf("NewRequest error: %v", err)
	}
	if req.Header.Get("X-Plex-Token") != "fake-token-123" {
		t.Fatalf("expected token header set")
	}
}

func TestDo404AndDecode(t *testing.T) {
	c, _ := NewClient("http://example.com", "fake-tok")
	// test 404 handling
	c.httpClient = http.Client{Transport: &simpleRT{status: 404, body: "{}"}, Timeout: time.Second}
	_, err := c.NewRequest("GET", "/notfound")
	if err != nil {
		t.Fatalf("unexpected NewRequest err: %v", err)
	}
	// Build a request and call Do directly
	req, _ := c.NewRequest("GET", "/notfound")
	err = c.Do(req, &struct{}{})
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// test decode
	c.httpClient = http.Client{Transport: &simpleRT{status: 200, body: `{"foo":"bar"}`}, Timeout: time.Second}
	req, _ = c.NewRequest("GET", "/ok")
	var out map[string]string
	if err := c.Do(req, &out); err != nil {
		t.Fatalf("expected decode success, got %v", err)
	}
	if out["foo"] != "bar" {
		t.Fatalf("unexpected decode result: %v", out)
	}
}

func TestDoRejectsNonSuccessfulStatus(t *testing.T) {
	c, err := NewClient("http://example.com", "fake-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.httpClient = http.Client{
		Transport: &simpleRT{status: http.StatusUnauthorized, body: `{"error":"bad token"}`},
		Timeout:   time.Second,
	}
	req, err := c.NewRequest(http.MethodGet, "/unauthorized")
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	err = c.Do(req, &struct{}{})
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("Do error = %v, want HTTPStatusError", err)
	}
	if statusErr.StatusCode != http.StatusUnauthorized || !strings.Contains(statusErr.Body, "bad token") {
		t.Fatalf("unexpected status error: %+v", statusErr)
	}
}

func TestDoDrainsSuccessfulResponseWithoutDecodeTarget(t *testing.T) {
	body := &trackingBody{reader: strings.NewReader("response payload")}
	c, err := NewClient("http://example.com", "fake-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.httpClient = http.Client{Transport: clientRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Status:     "204 No Content",
			Header:     make(http.Header),
			Body:       body,
			Request:    req,
		}, nil
	})}
	req, err := c.NewRequest(http.MethodGet, "/no-content")
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if err := c.Do(req, nil); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if !body.fullyRead || !body.closed {
		t.Fatalf("response body was not drained and closed: %+v", body)
	}
}

func TestDoDrainsSuccessfulResponseAfterDecode(t *testing.T) {
	body := &trackingBody{reader: strings.NewReader(`{"value":"ok"}` + strings.Repeat(" ", 8192))}
	c, err := NewClient("http://example.com", "fake-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.httpClient = http.Client{Transport: clientRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       body,
			Request:    req,
		}, nil
	})}
	req, err := c.NewRequest(http.MethodGet, "/decode")
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	var decoded map[string]string
	if err := c.Do(req, &decoded); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if decoded["value"] != "ok" {
		t.Fatalf("decoded value = %q, want ok", decoded["value"])
	}
	if !body.fullyRead || !body.closed {
		t.Fatalf("response body was not drained and closed: %+v", body)
	}
}

func TestGetWithHeadersReturnHeadersRejectsErrorStatus(t *testing.T) {
	c, err := NewClient("http://example.com", "fake-token")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.httpClient = http.Client{Transport: clientRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("X-Plex-Request-Id", "request-1")
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Status:     "500 Internal Server Error",
			Header:     header,
			Body:       io.NopCloser(strings.NewReader("temporary failure")),
			Request:    req,
		}, nil
	})}

	headers, err := c.GetWithHeadersReturnHeaders("/failure", &struct{}{}, nil)
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("error = %v, want HTTPStatusError with status 500", err)
	}
	if headers.Get("X-Plex-Request-Id") != "request-1" {
		t.Fatalf("response headers were not returned: %v", headers)
	}
}

func TestInviteFriendEndToEnd(t *testing.T) {
	// httptest server to accept the invite request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || !strings.HasSuffix(r.URL.Path, "/api/v2/shared_servers") {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}

		body, _ := io.ReadAll(r.Body)
		if !bytes.Contains(body, []byte("invitedEmail")) {
			t.Fatalf("request missing invitedEmail: %s", string(body))
		}

		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(`{"id":"1","ownerId":"2","invitedId":"3","serverId":"4","numLibraries":"0","invited":{"id":"5"},"sharingSettings":{"allowTuners":"0"},"libraries":[]}`)); err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
	}))
	defer srv.Close()

	// create a client that points to the test server
	p, err := ttPlex.New(srv.URL, "fake-token")
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	params := ttPlex.InviteFriendParams{UsernameOrEmail: "a@b.c", MachineID: "fake-machine", Label: "", LibraryIDs: []int{1}}
	if err := p.InviteFriend(params); err != nil {
		t.Fatalf("InviteFriend failed: %v", err)
	}
}
