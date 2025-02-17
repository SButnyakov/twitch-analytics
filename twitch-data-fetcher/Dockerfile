FROM golang:1.24.0-bookworm

WORKDIR /twitch-data-fetcher

COPY . .

RUN go mod download

RUN go build -o main .

CMD ["./main"]
