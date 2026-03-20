package offchain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type addResponse struct {
	Name string `json:"Name"`
	Hash string `json:"Hash"`
	Size string `json:"Size"`
}

type IPFSClient struct {
	apiBaseURL string
	httpClient *http.Client
}

func NewIPFSClient(apiBaseURL string, timeout time.Duration) *IPFSClient {
	base := strings.TrimSpace(apiBaseURL)
	base = strings.TrimRight(base, "/")

	return &IPFSClient{
		apiBaseURL: base,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *IPFSClient) AddBytes(name string, data []byte) (string, error) {
	if c == nil || c.httpClient == nil {
		return "", fmt.Errorf("ipfs client is not initialized")
	}
	if c.apiBaseURL == "" {
		return "", fmt.Errorf("ipfs api base url is empty")
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", name)
	if err != nil {
		return "", err
	}

	if _, err = part.Write(data); err != nil {
		return "", err
	}

	if err = writer.Close(); err != nil {
		return "", err
	}

	apiURL := c.apiBaseURL + "/api/v0/add?pin=true"
	req, err := http.NewRequest(http.MethodPost, apiURL, &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("ipfs add failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	lines := bytes.Split(bytes.TrimSpace(respBody), []byte("\n"))
	if len(lines) == 0 {
		return "", fmt.Errorf("ipfs add returned empty response")
	}
	lastLine := lines[len(lines)-1]

	result := addResponse{}
	if err = json.Unmarshal(lastLine, &result); err != nil {
		return "", fmt.Errorf("parse ipfs add response failed: %w", err)
	}
	if result.Hash == "" {
		return "", fmt.Errorf("ipfs add returned empty hash")
	}

	return result.Hash, nil
}

func (c *IPFSClient) Cat(cid string) ([]byte, error) {
	if c == nil || c.httpClient == nil {
		return nil, fmt.Errorf("ipfs client is not initialized")
	}
	if c.apiBaseURL == "" {
		return nil, fmt.Errorf("ipfs api base url is empty")
	}

	cid = strings.TrimSpace(cid)
	if cid == "" {
		return nil, fmt.Errorf("cid is empty")
	}

	query := url.Values{}
	query.Set("arg", cid)
	apiURL := c.apiBaseURL + "/api/v0/cat?" + query.Encode()

	req, err := http.NewRequest(http.MethodPost, apiURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("ipfs cat failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return body, nil
}
