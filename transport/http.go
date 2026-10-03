package transport

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"time"
)

// RPCPathPrefix is the URL prefix under which a Server serves RPCs over HTTP.
const RPCPathPrefix = "/rpc/"

// ServeHTTP exposes the server's handlers at POST /rpc/{method}.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	method := strings.TrimPrefix(r.URL.Path, RPCPathPrefix)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := s.Dispatch(method, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/x-gob")
	w.Write(resp)
}

// HTTPCaller is a Caller that sends RPCs to a remote Server over HTTP.
type HTTPCaller struct {
	baseURL string
	client  *http.Client
}

// NewHTTPCaller returns a Caller for the node at baseURL (for example
// "http://127.0.0.1:7001"). Calls that take longer than timeout fail.
func NewHTTPCaller(baseURL string, timeout time.Duration) *HTTPCaller {
	return &HTTPCaller{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

// Call implements Caller.
func (c *HTTPCaller) Call(method string, args any, reply any) bool {
	body, err := Encode(args)
	if err != nil {
		return false
	}
	resp, err := c.client.Post(c.baseURL+RPCPathPrefix+method, "application/x-gob", bytes.NewReader(body))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	return Decode(data, reply) == nil
}

// Close releases idle connections held by the caller.
func (c *HTTPCaller) Close() {
	c.client.CloseIdleConnections()
}
