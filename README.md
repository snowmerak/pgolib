# pgolib

pgolib is a golang library for profile guided optimization.

## Usage

```shell
go get github.com/snowmerak/pgolib
```

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/snowmerak/pgolib/profile"
	"github.com/snowmerak/pgolib/storage/minio"

	"signal"
	"os"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	strg, err := minio.NewClient(ctx, "practice-app", 10, "profile", &minio.Config{
		Endpoint: "localhost:9000",
	})
	if err != nil {
		panic(err)
	}

	prof := profile.New(strg, 30*time.Minute, 5*time.Minute) // 30 minutes for delay, 5 minutes for collect interval

	errCh, err := prof.Run(ctx)
	if err != nil {
		panic(err)
	}

	done := ctx.Done()
loop:
	for {
		select {
		case err := <-errCh:
			log.Printf("error: %v", err)
		case <-done:
			break loop
		}
	}
	
	log.Println("done")
}
```