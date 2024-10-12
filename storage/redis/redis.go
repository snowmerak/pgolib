package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/snowmerak/pgolib/slicex"
	"github.com/snowmerak/pgolib/storage"
)

var _ storage.Storage = (*Client)(nil)

type Client struct {
	client           rueidis.Client
	namespace        string
	appName          string
	expireTime       time.Duration
	maxProfileBuffer int64
}

type Config struct {
	Addresses []string
	Username  string
	Password  string
}

func New(ctx context.Context, namespace, appName string, ttl time.Duration, maxProfileBuffer int64, cfg *Config) (*Client, error) {
	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: cfg.Addresses,
		Username:    cfg.Username,
		Password:    cfg.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create redis client: %w", err)
	}

	context.AfterFunc(ctx, func() {
		client.Close()
	})

	return &Client{
		client:           client,
		namespace:        namespace,
		appName:          appName,
		expireTime:       ttl,
		maxProfileBuffer: maxProfileBuffer,
	}, nil
}

func (c *Client) makeKey(createdAt time.Time) string {
	return fmt.Sprintf("%s:%s:%d", c.namespace, c.appName, createdAt.UnixMilli())
}

func (c *Client) getCreatedAt(key string) (time.Time, error) {
	idx := len(key) - 1
	for idx >= 0 && key[idx] != ':' {
		idx--
	}

	lastSection := key[idx+1:]

	value, err := strconv.ParseInt(lastSection, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse key: %w", err)
	}

	return time.UnixMilli(value), nil
}

var keyPattern = atomic.Pointer[string]{}

func (c *Client) makeKeyPattern() *string {
	if p := keyPattern.Load(); p != nil {
		return p
	}

	value := fmt.Sprintf("%s:%s:*", c.namespace, c.appName)
	keyPattern.CompareAndSwap(nil, &value)

	return keyPattern.Load()
}

func (c *Client) SaveProfile(ctx context.Context, createdAt time.Time, profile []byte) error {
	key := c.makeKey(createdAt)
	if err := c.client.Do(ctx, c.client.B().Set().Key(key).Value(rueidis.BinaryString(profile)).Ex(c.expireTime).Build()).Error(); err != nil {
		return fmt.Errorf("failed to set profile: %w", err)
	}

	return nil
}

func (c *Client) GetProfile(ctx context.Context, createdAt time.Time) ([]byte, error) {
	key := c.makeKey(createdAt)
	resp, err := c.client.Do(ctx, c.client.B().Get().Key(key).Build()).AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get profile: %w", err)
	}

	return resp, nil
}

func (c *Client) GetProfiles(ctx context.Context, startedAt, endedAt time.Time) ([][]byte, error) {
	keyPattern := c.makeKeyPattern()

	result := make([][]byte, 0, 32)
	sizeList := make([]int64, 0, 32)
	inserted := -1

	cursor := uint64(0)
	for {
		resp, err := c.client.Do(ctx, c.client.B().Scan().Cursor(cursor).Match(*keyPattern).Build()).AsScanEntry()
		if err != nil {
			return nil, fmt.Errorf("failed to scan profiles: %w", err)
		}

		if len(resp.Elements) == 0 {
			break
		}

		for _, elem := range resp.Elements {
			createdAt, err := c.getCreatedAt(elem)
			if err != nil {
				return nil, fmt.Errorf("failed to get created at: %w", err)
			}

			if createdAt.Before(startedAt) || createdAt.After(endedAt) {
				continue
			}

			profile, err := c.GetProfile(ctx, createdAt)
			if err != nil {
				return nil, fmt.Errorf("failed to get profile: %w", err)
			}

			sizeList, inserted = slicex.InsertBinary(sizeList, int64(len(profile)), int(c.maxProfileBuffer))
			result = slicex.InsertAt(result, inserted, profile, int(c.maxProfileBuffer))
		}

		cursor = resp.Cursor
		if cursor == 0 {
			break
		}
	}

	return result, nil
}

func (c *Client) DeleteProfile(ctx context.Context, createdAt time.Time) error {
	key := c.makeKey(createdAt)
	if err := c.client.Do(ctx, c.client.B().Del().Key(key).Build()).Error(); err != nil {
		return fmt.Errorf("failed to delete profile: %w", err)
	}

	return nil
}

func (c *Client) DeleteProfiles(ctx context.Context, startedAt, endedAt time.Time) error {
	keyPattern := c.makeKeyPattern()

	cursor := uint64(0)
	for {
		resp, err := c.client.Do(ctx, c.client.B().Scan().Cursor(cursor).Match(*keyPattern).Build()).AsScanEntry()
		if err != nil {
			return fmt.Errorf("failed to scan profiles: %w", err)
		}

		if len(resp.Elements) == 0 {
			break
		}

		for _, elem := range resp.Elements {
			createdAt, err := c.getCreatedAt(elem)
			if err != nil {
				return fmt.Errorf("failed to get created at: %w", err)
			}

			if createdAt.Before(startedAt) || createdAt.After(endedAt) {
				continue
			}

			if err := c.DeleteProfile(ctx, createdAt); err != nil {
				return fmt.Errorf("failed to delete profile: %w", err)
			}
		}

		cursor = resp.Cursor
		if cursor == 0 {
			break
		}
	}

	return nil
}
