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

	period   time.Duration
	duration time.Duration
}

func NewProfiler(storage storage.Storage, period, duration time.Duration) *Profiler {
	return &Profiler{
		storage:  storage,
		period:   period,
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

func (p *Profiler) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancelFunc = cancel
	ticker := time.NewTicker(p.period)
	defer ticker.Stop()

	done := ctx.Done()

	for {
		select {
		case <-done:
			return nil
		case <-ticker.C:
			now := time.Now()
			pf, err := collectCpuProfile(p.duration)
			if err != nil {
				return fmt.Errorf("failed to collect CPU profile: %w", err)
			}

			if err := p.storage.SaveProfile(ctx, now, pf); err != nil {
				return fmt.Errorf("failed to save profile: %w", err)
			}
		}
	}
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
