package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
	"strconv"
	"strings"
	"time"
)

// EXPIRE 命令: 设置键在 seconds 秒后过期
func Expire(ctx *core.Context) protocol.Value {
	return expireGeneric(ctx, time.Second, "EXPIRE")
}

// PEXPIRE 命令: 设置键在 milliseconds 毫秒后过期
func PExpire(ctx *core.Context) protocol.Value {
	return expireGeneric(ctx, time.Millisecond, "PEXPIRE")
}

// 通用过期处理逻辑
func expireGeneric(ctx *core.Context, unit time.Duration, cmdName string) protocol.Value {
	if len(ctx.Args) != 2 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for '" + strings.ToLower(cmdName) + "' command"}
	}

	key := string(ctx.Args[0].Bulk)
	durationStr := string(ctx.Args[1].Bulk)

	durationVal, err := strconv.ParseInt(durationStr, 10, 64)
	if err != nil {
		return protocol.Value{Type: protocol.Error, Str: "ERR value is not an integer or out of range"}
	}
    // Redis 协议规定：过期时间必须大于等于 0（虽然 0 会导致立即过期，但在 SetExpiration 中通常处理为删除或不设置，这里我们允许设置）
    // 为了简单，我们遵循 Redis：负数通常报错或视为立即过期，这里简化处理。
    if durationVal <= 0 {
         // Redis 6.x+ 对负数 TTL 会直接删除 Key。这里为了简化，我们报错或视为非法。
         // 标准 Redis: EXPIRE key -1 会删除 key。
         // 这里我们暂时简单处理：只允许正数。
         return protocol.Value{Type: protocol.Error, Str: "ERR value must be positive"}
    }

	deadline := time.Now().Add(time.Duration(durationVal) * unit)

	found, _ := ctx.DB.SetExpiration(key, deadline)

	if found {
		// [AOF 写入] 只有当键存在且成功设置过期时间时，才记录 AOF
		if aofEngine := ctx.DB.GetAof(); aofEngine != nil {
			// 重组命令: [CMD, key, duration]
			cmd := protocol.Value{
				Type: protocol.Array,
				Array: []protocol.Value{
					{Type: protocol.BulkString, Bulk: []byte(cmdName)},
					ctx.Args[0],
					ctx.Args[1],
				},
			}
			aofEngine.Write(toRespBytes(cmd))
		}
		return protocol.Value{Type: protocol.Integer, Num: 1}
	}
	return protocol.Value{Type: protocol.Integer, Num: 0} // Key 不存在
}

// TTL 命令: 以秒为单位返回剩余时间
func TTL(ctx *core.Context) protocol.Value {
	return ttlGeneric(ctx, time.Second)
}

// PTTL 命令: 以毫秒为单位返回剩余时间
func PTTL(ctx *core.Context) protocol.Value {
	return ttlGeneric(ctx, time.Millisecond)
}

// 通用 TTL 处理逻辑
func ttlGeneric(ctx *core.Context, unit time.Duration) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for command"}
	}

	key := string(ctx.Args[0].Bulk)

	duration, found, _ := ctx.DB.GetTTL(key)

	if !found {
		return protocol.Value{Type: protocol.Integer, Num: -2} // 键不存在
	}

	if duration == 0 {
		return protocol.Value{Type: protocol.Integer, Num: -1} // 键存在但无过期时间
	}

	// 转换时间单位
	var t int64
	if unit == time.Second {
		t = int64(duration.Seconds())
	} else {
		t = int64(duration.Milliseconds())
	}

    // 修正：如果刚过期的瞬间，可能计算出 -1 或 0，为了符合 Redis 语义 (-2 已处理)，这里保持非负
	if t < 0 {
		t = 0
	}

	return protocol.Value{Type: protocol.Integer, Num: t}
}

// PERSIST 命令: 移除过期时间
func Persist(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for 'persist' command"}
	}

	key := string(ctx.Args[0].Bulk)

	removed, _ := ctx.DB.RmExpiration(key)

	if removed {
		// [AOF 写入]
		if aofEngine := ctx.DB.GetAof(); aofEngine != nil {
			cmd := protocol.Value{
				Type: protocol.Array,
				Array: []protocol.Value{
					{Type: protocol.BulkString, Bulk: []byte("PERSIST")},
					ctx.Args[0],
				},
			}
			aofEngine.Write(toRespBytes(cmd))
		}
		return protocol.Value{Type: protocol.Integer, Num: 1}
	}

	return protocol.Value{Type: protocol.Integer, Num: 0} // 键不存在或没有过期时间
}

// 本地辅助函数：将 protocol.Value 转换为 RESP 字节流
// (如果你的 pkg/protocol 包里没有公开 ToRespBytes，可以在这里加一个)
func ToRespBytes(v protocol.Value) []byte {
	var sb strings.Builder
	sb.WriteString(protocol.MakeArrayHeader(len(v.Array)))
	for _, item := range v.Array {
		sb.WriteString(protocol.MakeBulkString(string(item.Bulk)))
	}
	return []byte(sb.String())
}