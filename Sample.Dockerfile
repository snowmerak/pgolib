FROM golang:1.23 AS builder
LABEL authors="<your-name>"

ARG PGO=off

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the Go app
RUN CGO_ENABLED=0 go build -pgo=$PGO -o ./build/app ./cmd/app/.

FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/build/app .

CMD ["./app"]
