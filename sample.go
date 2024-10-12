package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/snowmerak/pgolib/profile"
	"github.com/snowmerak/pgolib/storage/minio"
)

func main() {
	const (
		appName = "sample"
	)

	var (
		endedAt   = time.Now()
		startedAt = endedAt.Add(-24 * time.Hour)
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	strg, err := minio.New(ctx, appName, 32, "profile", &minio.Config{
		Endpoint:        "localhost:9000",
		Bucket:          "profile",
		AccessKeyID:     "minio",
		SecretAccessKey: "minio123",
	})
	if err != nil {
		panic(err)
	}

	profiler := profile.NewProfiler(strg, 0, 0)
	data, err := profiler.GetProfile(ctx, startedAt, endedAt)
	if err != nil {
		panic(err)
	}

	f, err := os.Create("profile.pprof")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	defer f.Sync()

	if _, err := f.Write(data); err != nil {
		panic(err)
	}
}
