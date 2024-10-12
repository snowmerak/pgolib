package localdir

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/snowmerak/pgolib/slicex"
	"github.com/snowmerak/pgolib/storage"
)

type LocalDir struct {
	appName             string
	prefixDir           string
	profileBufferLength int64
}

func New(appName string, prefixDir string, profileBufferLength int64) (*LocalDir, error) {
	if err := os.MkdirAll(prefixDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	return &LocalDir{
		appName:             appName,
		prefixDir:           prefixDir,
		profileBufferLength: profileBufferLength,
	}, nil
}

func (l *LocalDir) SaveProfile(_ context.Context, createdAt time.Time, data []byte) error {
	path := filepath.Join(l.prefixDir, storage.MakeFilename(l.appName, createdAt))
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync data: %w", err)
	}

	return nil
}

func (l *LocalDir) GetProfile(_ context.Context, createdAt time.Time) ([]byte, error) {
	path := filepath.Join(l.prefixDir, storage.MakeFilename(l.appName, createdAt))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

func (l *LocalDir) GetProfiles(_ context.Context, startedAt, endedAt time.Time) ([][]byte, error) {
	entries, err := os.ReadDir(l.prefixDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	profiles := make([][]byte, 0, len(entries))
	sizeList := make([]int64, 0, len(entries))
loop:
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), storage.Extension) {
			continue loop
		}

		info, err := entry.Info()
		if err != nil {
			continue loop
		}

		if info.ModTime().Before(startedAt) || info.ModTime().After(endedAt) {
			continue loop
		}

		data, err := os.ReadFile(filepath.Join(l.prefixDir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}

		profiles = append(profiles)
		sizeList = append(sizeList)
		inserted := -1

		sizeList, inserted = slicex.InsertBinary(sizeList, info.Size(), int(l.profileBufferLength))
		if inserted == -1 {
			continue loop
		}

		profiles = slicex.InsertAt(profiles, inserted, data, int(l.profileBufferLength))
	}

	return profiles, nil
}

func (l *LocalDir) DeleteProfile(ctx context.Context, createdAt time.Time) error {
	path := filepath.Join(l.prefixDir, storage.MakeFilename(l.appName, createdAt))
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to remove file: %w", err)
	}

	return nil
}

func (l *LocalDir) DeleteProfiles(ctx context.Context, startedAt, endedAt time.Time) error {
	entries, err := os.ReadDir(l.prefixDir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), storage.Extension) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(startedAt) || info.ModTime().After(endedAt) {
			continue
		}

		if err := os.Remove(filepath.Join(l.prefixDir, entry.Name())); err != nil {
			return fmt.Errorf("failed to remove file: %w", err)
		}
	}

	return nil
}
