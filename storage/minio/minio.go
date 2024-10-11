package minio

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/snowmerak/pgolib/storage"
)

var _ storage.Storage = (*Client)(nil)

type Client struct {
	appName             string
	keyPrefix           string
	bucket              string
	profileBufferLength int64
	client              *minio.Client
}

type Config struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	Token           string
	UseSSL          bool
}

func New(_ context.Context, appName string, profileBufferLength int64, keyPrefix string, cfg *Config) (*Client, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.Token),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	if !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}

	return &Client{
		appName:             appName,
		keyPrefix:           keyPrefix,
		bucket:              cfg.Bucket,
		profileBufferLength: profileBufferLength,
		client:              client,
	}, nil
}

func (c *Client) SaveProfile(ctx context.Context, createdAt time.Time, profile []byte) error {
	key := c.keyPrefix + storage.MakeFilename(c.keyPrefix, createdAt)
	_, err := c.client.PutObject(ctx, c.bucket, key, bytes.NewReader(profile), int64(len(profile)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return fmt.Errorf("failed to save profile: %w", err)
	}

	return nil
}

func (c *Client) GetProfile(ctx context.Context, createdAt time.Time) ([]byte, error) {
	key := c.keyPrefix + storage.MakeFilename(c.keyPrefix, createdAt)
	obj, err := c.client.GetObject(ctx, c.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get profile: %w", err)
	}
	defer obj.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(obj); err != nil {
		return nil, fmt.Errorf("failed to read profile: %w", err)
	}

	return buf.Bytes(), nil
}

type Metadata struct {
	key  string
	size int64
}

func (c *Client) GetProfiles(ctx context.Context, startedAt, endedAt time.Time) ([][]byte, error) {
	ch := c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
		Prefix:       c.keyPrefix,
		WithMetadata: true,
	})
	metadataList := make([]Metadata, 0, c.profileBufferLength+1)

	for obj := range ch {
		if obj.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", obj.Err)
		}

		if obj.LastModified.Before(startedAt) || obj.LastModified.After(endedAt) {
			continue
		}

		metadataList = append(metadataList, Metadata{
			key:  obj.Key,
			size: obj.Size,
		})
		slices.SortFunc(metadataList, func(a, b Metadata) int {
			switch {
			case a.size < b.size:
				return 1
			case a.size > b.size:
				return -1
			default:
				return 0
			}
		})

		if len(metadataList) > int(c.profileBufferLength) {
			metadataList = metadataList[:len(metadataList)-1]
		}
	}

	profiles := make([][]byte, len(metadataList))
	for _, metadata := range metadataList {
		profile, err := c.client.GetObject(ctx, c.bucket, metadata.key, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get profile: %w", err)
		}

		buf := new(bytes.Buffer)
		if _, err := buf.ReadFrom(profile); err != nil {
			return nil, fmt.Errorf("failed to read profile: %w", err)
		}

		profiles = append(profiles, buf.Bytes())
	}

	return profiles, nil
}

func (c *Client) DeleteProfile(ctx context.Context, createdAt time.Time) error {
	key := c.keyPrefix + storage.MakeFilename(c.keyPrefix, createdAt)
	err := c.client.RemoveObject(ctx, c.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete profile: %w", err)
	}

	return nil
}

func (c *Client) DeleteProfiles(ctx context.Context, startedAt, endedAt time.Time) error {
	ch := c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
		Prefix: c.keyPrefix,
	})
	for obj := range ch {
		if obj.Err != nil {
			return fmt.Errorf("failed to list objects: %w", obj.Err)
		}

		if obj.LastModified.Before(startedAt) || obj.LastModified.After(endedAt) {
			continue
		}

		err := c.client.RemoveObject(ctx, c.bucket, obj.Key, minio.RemoveObjectOptions{})
		if err != nil {
			return fmt.Errorf("failed to delete profile: %w", err)
		}
	}

	return nil
}
