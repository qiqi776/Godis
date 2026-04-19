# Godis

Godis 是一个用 Go 从零实现的缓存项目

## 当前已完成

- TCP server 启动链路
- RESP2 基础解析
- `PING`
- `GET`
- `SET`
- `DEL`
- `EXISTS`
- `EXPIRE`
- `TTL`
- `PERSIST`
- `SELECT`
- unknown command 错误返回
- 优雅退出
- 基础集成测试
- 新增命令注册表 + arity校验
