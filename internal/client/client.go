package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

type Client struct {
	SocketPath string
	Timeout    time.Duration
	sequence   atomic.Uint64
}

func New(socketPath string) *Client {
	return &Client{SocketPath: socketPath, Timeout: 3 * time.Second}
}

func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	data, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode params: %w", err)
	}
	request := protocol.Request{ID: fmt.Sprintf("cli:%d", c.sequence.Add(1)), Method: method, Params: data}
	dialer := net.Dialer{Timeout: c.Timeout}
	connection, err := dialer.DialContext(ctx, "unix", c.SocketPath)
	if err != nil {
		return fmt.Errorf("connect to agent-bridge: %w", err)
	}
	defer connection.Close()
	deadline := time.Now().Add(c.Timeout)
	_ = connection.SetDeadline(deadline)
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	var response protocol.Response
	if err := json.NewDecoder(bufio.NewReader(connection)).Decode(&response); err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if response.Error != nil {
		return fmt.Errorf("%s: %s", response.Error.Code, response.Error.Message)
	}
	if result == nil {
		return nil
	}
	encoded, err := json.Marshal(response.Result)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(encoded, result); err != nil {
		return fmt.Errorf("decode result: %w", err)
	}
	return nil
}
