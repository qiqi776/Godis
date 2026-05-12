# mini-kv

[![Go](https://img.shields.io/badge/Go-1.26.2-00ADD8)](https://go.dev/)

mini-kv 是一个基于 Raft 的分布式 KV 存储学习项目

## Getting Started

### 环境要求

- Go 1.26.2 或兼容版本
- Bash，用于运行 `scripts/bench/*`
- 本地回环网络端口，用于 gRPC、Raft transport 和 debug HTTP

## Features / Specification

### 当前能力

- gRPC RawKV API：`Get`、`Set`、`Delete`
- 单 Raft Group：leader election、log replication、commit/apply
- Raft 持久化：hard state、log、snapshot WAL
- 状态机 snapshot：创建、安装、恢复
- ReadIndex：leader 线性读入口
- 可观测性：应用层 metrics、debug HTTP、bench 采集脚本
- 压测与故障注入：workload matrix、leader kill、follower restart、snapshot catch-up
- 正确性工具：`tests/perfkit` 中包含 workload、fault transport、porcupine checker、regression helper
