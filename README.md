# mini-kv

mini-kv 是一个用 Go 实现的分布式 KV 学习项目。

## 当前能力

- 单 Group Raft：leader election、log replication、commit/apply
- Raft 日志持久化：`internal/raft/logstore`
- protobuf/gRPC：最小可用 KV 服务入口
- 对外命令：`GET` `SET` `DELETE`
- Snapshot：状态机 snapshot 与安装恢复
- Fake transport 和持久化场景测试

## 当前分层

- `api/minikv/v1`：protobuf/gRPC API 定义与生成代码
- `internal/server/grpcserver`：gRPC server 适配层
- `internal/service/minikv`：最小 KV 服务层
- `internal/raftstore`：Raft 支撑的复制型 store runtime
- `internal/kv`：当前单组 KV 状态机语义
- `internal/raft`：Raft 算法与协议

## Run

```bash
go run ./cmd/mini-kv
```

## Test

```bash
go test ./...
```
