package storage

import (
	"context"
	"fmt"
	"time"
)

// Extension is the file extension for the profile data.
const Extension = ".pprof"

// MakeFilename generates a filename for the profile data.
func MakeFilename(prefix string, createdAt time.Time) string {
	return fmt.Sprintf("%s-%s%s", prefix, createdAt.Format(time.RFC3339), Extension)
}

// Storage is an interface that defines the methods that a storage system for golang profiles for PGO can implement.
type Storage interface {
	// SaveProfile saves the profile data to the storage system.
	SaveProfile(ctx context.Context, createdAt time.Time, profile []byte) error
	// GetProfile retrieves the profile data from the storage system.
	GetProfile(ctx context.Context, createdAt time.Time) ([]byte, error)
	// GetProfiles retrieves the profile data from the storage system.
	GetProfiles(ctx context.Context, startedAt, endedAt time.Time) ([][]byte, error)
	// DeleteProfile deletes the profile data from the storage system.
	DeleteProfile(ctx context.Context, createdAt time.Time) error
	// DeleteProfiles deletes the profile data from the storage system.
	DeleteProfiles(ctx context.Context, startedAt, endedAt time.Time) error
}
