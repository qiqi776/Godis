# 1. 构建阶段
FROM golang:1.25 AS builder

WORKDIR /app

# 利用 Docker 缓存机制，先下载依赖
COPY go.mod ./
# COPY go.sum ./
# RUN go mod download

# 复制源码
COPY . .

# 编译 (CGO_ENABLED=0 生成静态二进制)
RUN CGO_ENABLED=0 GOOS=linux go build -o godis cmd/godis/main.go

# 2. 运行阶段 (使用极小的镜像)
FROM alpine:latest

WORKDIR /root/

# 从构建阶段复制二进制文件
COPY --from=builder /app/godis .

# 暴露端口
EXPOSE 6379

CMD ["./godis"]