# Godis

Godis 是一个用 Go 从零实现的缓存项目，目前处在单机阶段

## 已完成

- TCP server
- RESP2 parser
- multi DB
- TTL
- command registry
- string
- list
- hash
- set
- zset
- bitmap

## Run

```bash
go run ./cmd/godis
```

## Test

```bash
go test ./...
```
