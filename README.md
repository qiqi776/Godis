# Godis

Godis 是一个用 Go 从零实现的 Redis-like 缓存项目，目前处在单机阶段

## 已完成

- TCP server
- RESP2 parser / reply
- multi DB
- TTL
- command registry
- string
- list
- hash
- set
- zset
- bitmap
- transaction
- pub/sub
- system commands

## 当前支持的命令

- 基础命令：`PING` `GET` `SET` `DEL` `EXISTS`
- 过期控制：`EXPIRE` `TTL` `PERSIST`
- 数据库切换：`SELECT`
- list：`LPUSH` `RPUSH` `LPOP` `RPOP` `LRANGE`
- hash：`HSET` `HGET` `HDEL` `HGETALL`
- set：`SADD` `SREM` `SMEMBERS` `SISMEMBER`
- zset：`ZADD` `ZREM` `ZRANGE` `ZSCORE`
- bitmap：`SETBIT` `GETBIT` `BITCOUNT`
- 事务：`MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH`
- Pub/Sub：`SUBSCRIBE` `UNSUBSCRIBE` `PUBLISH`
- 系统命令：`TYPE` `DBSIZE` `INFO` `COMMAND`

## Run

```bash
go run ./cmd/godis
```

## Test

```bash
go test ./...
```

## Benchmark

```bash
go test ./tests/integration -run '^$' -bench . -benchmem
```
