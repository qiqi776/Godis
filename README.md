# Godis

Godis 是一个用 Go 从零实现的缓存项目

## 当前已完成

- TCP server 启动链路
- RESP2 基础解析
- `PING` 命令
- unknown command 错误返回
- 优雅退出
- 基础集成测试

## 当前支持的命令

- `PING`

其他命令暂未实现，会返回：

```text
ERR unknown command '<name>'
```

## 运行

在项目根目录执行：

```bash
go run ./cmd/godis
```

## 验证

新开一个终端：

```bash
redis-cli -p 6380 ping
```

也可以试一个未知命令：

```bash
redis-cli -p 6380 nope
```

## 测试

```bash
go test ./...
```
