package auth

import (
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
)

type Client struct {
	*http.Client
}

// NewClient creates an HTTP client with an optional proxy
// All clients share the same cookie jar passed in, one login for many proxies
func NewClient(jar http.CookieJar, proxyURL string) *Client {
	transport := &http.Transport{}
	if proxyURL != "" {
		parsed, _ := url.Parse(proxyURL)
		transport.Proxy = http.ProxyURL(parsed)
	}
	return &Client{
		Client: &http.Client{
			Jar:       jar,
			Transport: transport,
		},
	}
}

// NewJar creates a shared cookie jar for all workers
func NewJar() http.CookieJar {
	jar, _ := cookiejar.New(nil)
	return jar
}

func (c *Client) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	return c.Do(req)
}

func (c *Client) Login(loginURL, username, password string) error {
	resp, err := c.PostForm(loginURL, url.Values{
		"username":     {username},
		"password":     {password},
		"submit_login": {"Sign In"},
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("login error: %s", resp.Status)
	}
	return nil
}
