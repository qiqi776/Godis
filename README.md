# Godis

Godis 是一个用 Go 从零实现的缓存项目

## 当前主线

- RaftKV 总计划：`docs/raftkv-migration-plan.md`
- 文档索引：`docs/README.md`
- 当前阶段：R1 KV FSM

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
- AOF persistence
- RDB persistence
- hybrid AOF RDB preamble

## RaftKV 目前命令

- 读命令：`GET` `EXISTS` `TTL`
- 写命令：`SET` `DEL` `EXPIRE` `PERSIST`

Raft 模式下，所有写命令必须先写入 Raft log，并在 majority commit 后 apply 到 KV FSM。

## Standalone 兼容命令

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
- 持久化：`BGREWRITEAOF` `SAVE` `BGSAVE`

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
