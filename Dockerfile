FROM golang:1.26.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/wager-api ./cmd/wager-api \
    && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/migrate ./cmd/migrate

FROM alpine:3.22.1
RUN apk add --no-cache ca-certificates \
    && addgroup -g 10001 wager && adduser -D -u 10001 -G wager wager
WORKDIR /app
COPY --from=build /out/ /app/
COPY migrations/ /app/migrations/
COPY --chmod=755 infra/entrypoint.sh /app/entrypoint.sh
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/app/wager-api"]
