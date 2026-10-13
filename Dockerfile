# Stage 1: Build static binary
FROM golang:1.24-alpine AS build

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s" -o main .

# Stage 2: Scratch - zero OS packages, zero CVE surface
FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

WORKDIR /app

COPY --from=build /app/main .

ENV PORT=8080
EXPOSE $PORT

ENTRYPOINT ["./main"]
