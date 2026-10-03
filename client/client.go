// Package client is a Go client for the key/value store's HTTP API.
//
// It remembers which node is the leader, follows redirects to the new leader
// after a failover, retries through elections, and tags every write with a
// client ID and sequence number so that a retried write is applied exactly
// once.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to a cluster. A Client issues one request at a time (its
// sequence numbers must be applied in order); use one Client per goroutine.
type Client struct {
	nodes    []string
	leader   int
	http     *http.Client
	clientID int64
	seq      int64
}

// New returns a client for the cluster whose nodes have the given base URLs.
func New(nodes []string) *Client {
	trimmed := make([]string, len(nodes))
	for i, n := range nodes {
		trimmed[i] = strings.TrimRight(n, "/")
	}
	return &Client{
		nodes: trimmed,
		http: &http.Client{
			Transport: &http.Transport{MaxIdleConnsPerHost: 4},
			// Redirects point at the leader; handle them ourselves so we
			// can remember it.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		clientID: rand.Int64N(1<<62) + 1,
	}
}

// ErrNotFound is returned by Get for a missing key.
var ErrNotFound = errors.New("key not found")

// Get returns the value of key, or ErrNotFound.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	status, body, err := c.do(ctx, http.MethodGet, key, "")
	if err != nil {
		return "", err
	}
	if status == http.StatusNotFound {
		return "", ErrNotFound
	}
	return body, nil
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value string) error {
	_, _, err := c.do(ctx, http.MethodPut, key, value)
	return err
}

// Delete removes key.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, _, err := c.do(ctx, http.MethodDelete, key, "")
	return err
}

// Leader returns the URL of the node the client currently believes leads.
func (c *Client) Leader() string { return c.nodes[c.leader] }

func (c *Client) do(ctx context.Context, method, key, value string) (int, string, error) {
	var seq int64
	if method != http.MethodGet {
		c.seq++
		seq = c.seq
	}
	path := "/kv/" + url.PathEscape(key)
	backoff := 5 * time.Millisecond
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, "", err
		}
		status, body, loc, err := c.try(ctx, c.nodes[c.leader]+path, method, value, seq)
		switch {
		case err == nil && (status == http.StatusOK || status == http.StatusNoContent || status == http.StatusNotFound):
			return status, body, nil
		case err == nil && status == http.StatusTemporaryRedirect && c.adoptLeader(loc):
			continue // retry immediately at the leader
		case err == nil && status >= 400 && status < 500:
			return status, "", fmt.Errorf("%s %s: %d %s", method, key, status, strings.TrimSpace(body))
		}
		// Node down, no leader yet, or timed out: try the next node.
		c.leader = (c.leader + 1) % len(c.nodes)
		if attempt%len(c.nodes) == 0 {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return 0, "", ctx.Err()
			}
			backoff = min(2*backoff, 50*time.Millisecond)
		}
	}
}

func (c *Client) try(ctx context.Context, target, method, value string, seq int64) (status int, body, location string, err error) {
	var rd io.Reader
	if method == http.MethodPut {
		rd = strings.NewReader(value)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, rd)
	if err != nil {
		return 0, "", "", err
	}
	req.Header.Set("X-Client-ID", strconv.FormatInt(c.clientID, 10))
	req.Header.Set("X-Request-Seq", strconv.FormatInt(seq, 10))
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, "", "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", "", err
	}
	return resp.StatusCode, string(b), resp.Header.Get("Location"), nil
}

// adoptLeader points the client at the node a redirect named.
func (c *Client) adoptLeader(location string) bool {
	for i, n := range c.nodes {
		if strings.HasPrefix(location, n+"/") && i != c.leader {
			c.leader = i
			return true
		}
	}
	return false
}
