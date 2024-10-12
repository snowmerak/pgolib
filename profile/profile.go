package profile

import (
	"bytes"
	"context"
	"fmt"
	"runtime/pprof"
	"time"

	"github.com/google/pprof/profile"

	"github.com/snowmerak/pgolib/storage"
)

type Profiler struct {
	storage storage.Storage

	cancelFunc context.CancelFunc

	interval time.Duration
	duration time.Duration
}

func NewProfiler(storage storage.Storage, interval, duration time.Duration) *Profiler {
	return &Profiler{
		storage:  storage,
		interval: interval,
		duration: duration,
	}
}

func collectCpuProfile(duration time.Duration) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := pprof.StartCPUProfile(buf); err != nil {
		return nil, fmt.Errorf("failed to start CPU profile: %w", err)
	}

	time.Sleep(duration)

	pprof.StopCPUProfile()

	return buf.Bytes(), nil
}

func (p *Profiler) Run(ctx context.Context) (<-chan error, error) {
	ctx, cancel := context.WithCancel(ctx)
	p.cancelFunc = cancel
	ticker := time.NewTicker(p.interval)

	done := ctx.Done()

	errCh := make(chan error, 32)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				go func() {
					now := time.Now()
					pf, err := collectCpuProfile(p.duration)
					if err != nil {
						errCh <- fmt.Errorf("failed to collect CPU profile: %w", err)
						return
					}

					if err := p.storage.SaveProfile(ctx, now, pf); err != nil {
						errCh <- fmt.Errorf("failed to save profile: %w", err)
						return
					}
				}()
			}
		}
	}()

	return errCh, nil
}

func (p *Profiler) Stop() {
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
}

func (p *Profiler) GetProfile(ctx context.Context, startedAt, endedAt time.Time) ([]byte, error) {
	rawProfiles, err := p.storage.GetProfiles(ctx, startedAt, endedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to get profiles: %w", err)
	}

	profiles := make([]*profile.Profile, 0, len(rawProfiles))
	for _, rawProfile := range rawProfiles {
		pf, err := profile.ParseData(rawProfile)
		if err != nil {
			return nil, fmt.Errorf("failed to parse profile: %w", err)
		}
		profiles = append(profiles, pf)
	}

	value, err := profile.Merge(profiles)
	if err != nil {
		return nil, fmt.Errorf("failed to merge profiles: %w", err)
	}

	buf := new(bytes.Buffer)
	if err := value.Write(buf); err != nil {
		return nil, fmt.Errorf("failed to write profile: %w", err)
	}

	return buf.Bytes(), nil
}
