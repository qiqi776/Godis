# mini-kv

mini-kv 是一个用 Go 实现的分布式 KV 学习项目

## 当前能力

- 单 Group Raft：leader election、log replication、commit/apply
- Raft 日志持久化：`internal/raft/logstore`
- protobuf/gRPC：KV 服务入口
- 对外命令：`GET` `SET` `DELETE`
- Snapshot：状态机 snapshot 与安装恢复
- Fake transport 和持久化场景测试

## Run

```bash
go run ./cmd/mini-kv
```

## Test

```bash
go test ./...
```

## Bench

启动本地 3 节点压测集群：

```bash
./scripts/bench/start.sh steady
```

运行一轮默认 mixed 压测并自动停机：

```bash
./scripts/bench/run.sh steady
```

运行默认 workload matrix 并生成 `summary.md` / `summary.json`：

```bash
./scripts/bench/run.sh steady --matrix default
```

压测结束后会自动抓取每个节点的 `/metrics`、`/debug/vars`、goroutine 和 heap 信息到 `tmp/bench/results/.../observability/`。

如果需要额外抓 CPU profile 和 trace：

```bash
./scripts/bench/run.sh steady --pprof-seconds 10
```

直接调用 bench CLI：

```bash
go run ./cmd/mini-kv-bench \
  -endpoints 127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382 \
  -routing leader \
  -mode mixed \
  -duration 30s \
  -warmup 5s \
  -concurrency 32
```

本地 bench profile 默认打开 debug HTTP：

- `steady`: `127.0.0.1:17080` / `17081` / `17082`
- `snapshot-stress`: `127.0.0.1:17180` / `17181` / `17182`

压测工具库放在 `tests/perfkit`，包含外部化 workload、`raft.Transport` 故障注入代理、porcupine 线性一致性 checker 和基线回归对比 helper。`internal/raft` 不依赖 observability，leader/commit 指标由应用层 sampler 通过公开接口采集。

## Faults

运行 leader kill 故障场景：

```bash
./scripts/bench/faults.sh leader-kill --profile steady
```

运行所有故障场景：

```bash
./scripts/bench/faults.sh all --profile steady --snapshot-profile snapshot-stress
```

故障场景会为每个 scenario 生成 `result.json`、`fault.json`、观测抓取结果，以及聚合 `summary.md` / `summary.json`。
