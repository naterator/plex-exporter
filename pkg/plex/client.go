package plex

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var ErrNotFound = errors.New("not found")

const maxErrorBodyBytes = 4 << 10

// HTTPStatusError reports a non-successful response from the Plex API.
// Response bodies are bounded so a broken or malicious endpoint cannot make
// an error path consume unbounded memory.
type HTTPStatusError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *HTTPStatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("plex API returned %s", e.Status)
	}
	return fmt.Sprintf("plex API returned %s: %s", e.Status, e.Body)
}

type Client struct {
	Token string
	URL   *url.URL

	httpClient http.Client
}

func NewClient(serverURL, token string) (*Client, error) {
	parsed, err := url.Parse(strings.TrimSpace(serverURL))
	if err != nil {
		return nil, fmt.Errorf("parse Plex server URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("plex server URL must use http or https")
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("plex server URL must include a host")
	}

	// Configure HTTP client and optional TLS skip verification for self-signed
	// or mismatched certificates. Honor SKIP_TLS_VERIFICATION env var when set
	// to "1" or "true". Also allow a configurable timeout via
	// PLEX_CLIENT_TIMEOUT_SECONDS (seconds). Default to 10s to tolerate
	// slower LAN responses and large libraries.
	httpClient := http.Client{}

	// Default timeout
	timeout := 10 * time.Second
	if v := os.Getenv("PLEX_CLIENT_TIMEOUT_SECONDS"); v != "" {
		if seconds, err := strconv.ParseInt(v, 10, 64); err == nil &&
			seconds > 0 && seconds <= int64(^uint64(0)>>1)/int64(time.Second) {
			timeout = time.Duration(seconds) * time.Second
		}
	}
	httpClient.Timeout = timeout

	if skipTLS, err := strconv.ParseBool(os.Getenv("SKIP_TLS_VERIFICATION")); err == nil && skipTLS {
		transport := &http.Transport{}
		if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
			transport = defaultTransport.Clone()
		}
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		} else {
			transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		}
		// nolint:gosec // InsecureSkipVerify is explicit and controlled by SKIP_TLS_VERIFICATION env var for testing/trusted networks
		transport.TLSClientConfig.InsecureSkipVerify = true
		httpClient.Transport = transport
	}

	client := &Client{
		Token:      token,
		URL:        parsed,
		httpClient: httpClient,
	}

	return client, nil
}

func (c *Client) NewRequest(method, path string) (*http.Request, error) {
	requestPath, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	reqURL := c.URL.ResolveReference(requestPath)
	req, err := http.NewRequest(method, reqURL.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Plex-Token", c.Token)

	return req, nil
}

func (c *Client) Do(request *http.Request, data any) error {
	// The request URL is derived from the Plex server address supplied by the
	// process operator; contacting that private or local address is the purpose
	// of this exporter.
	//nolint:gosec // G704: operator-controlled Plex endpoint, not untrusted input.
	resp, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	return decodeResponse(resp, data)
}

func (c *Client) Get(path string, data any) error {
	req, err := c.NewRequest("GET", path)
	if err != nil {
		return err
	}

	// Pass the target directly (don't take address of the interface parameter).
	return c.Do(req, data)
}

// GetWithHeaders performs a GET request like Get but allows callers to supply
// additional request headers. Useful for Plex headers such as
// X-Plex-Container-Start and X-Plex-Container-Size to request paged or
// container-only responses.
func (c *Client) GetWithHeaders(path string, data any, headers map[string]string) error {
	req, err := c.NewRequest("GET", path)
	if err != nil {
		return err
	}

	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}

	return c.Do(req, data)
}

// GetWithHeadersReturnHeaders performs a GET with additional request headers,
// decodes the response into data and returns the response headers (copied)
// so callers can read values like x-plex-container-total-size without
// needing to access the raw http.Response.
func (c *Client) GetWithHeadersReturnHeaders(path string, data any, headers map[string]string) (http.Header, error) {
	req, err := c.NewRequest("GET", path)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		if v != "" {
			req.Header.Set(k, v)
		}
	}

	// NewRequest resolves this path against the operator-configured Plex server.
	//nolint:gosec // G704: operator-controlled Plex endpoint, not untrusted input.
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	responseHeaders := resp.Header.Clone()
	return responseHeaders, decodeResponse(resp, data)
}

func decodeResponse(resp *http.Response, data any) error {
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return ErrNotFound
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		_, _ = io.Copy(io.Discard, resp.Body)
		status := resp.Status
		if status == "" {
			status = fmt.Sprintf("%d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
		}
		return &HTTPStatusError{
			StatusCode: resp.StatusCode,
			Status:     strings.TrimSpace(status),
			Body:       strings.TrimSpace(string(body)),
		}
	}

	// Decode directly from the response body into the provided target to
	// avoid buffering successful responses in memory. Draining a response when
	// no target is supplied allows the HTTP transport to reuse the connection.
	if data == nil {
		_, err := io.Copy(io.Discard, resp.Body)
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(data); err != nil {
		return err
	}
	_, err := io.Copy(io.Discard, resp.Body)
	return err
}
