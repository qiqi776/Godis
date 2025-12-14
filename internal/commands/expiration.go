package commands

import (
	"godis/internal/core"
	"godis/pkg/protocol"
	"strconv"
	"strings"
	"time"
)

// 设置键在seconds秒后过期
func Expire(ctx *core.Context) protocol.Value {
	return expireGeneric(ctx, time.Second, "EXPIRE")
}

// 设置键在milliseconds毫秒后过期
func PExpire(ctx *core.Context) protocol.Value {
	return expireGeneric(ctx, time.Millisecond, "PEXPIRE")
}

// 过期处理
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
    // Redis协议规定：过期时间必须大于等于 0（虽然 0 会导致立即过期，但在 SetExpiration 中通常处理为删除或不设置，这里我们允许设置）
    // 为了简单，我们遵循Redis：负数通常报错或视为立即过期，这里简化处理。
    if durationVal <= 0 {
         return protocol.Value{Type: protocol.Error, Str: "ERR value must be positive"}
    }

	deadline := time.Now().Add(time.Duration(durationVal) * unit)

	found, _ := ctx.DB.SetExpiration(key, deadline)

	if found {
		// 只有当键存在且成功设置过期时间时，才记录 AOF
		if aofEngine := ctx.DB.GetAof(); aofEngine != nil {
			cmd := protocol.Value{
				Type: protocol.Array,
				Array: []protocol.Value{
					{Type: protocol.BulkString, Bulk: []byte(cmdName)},
					ctx.Args[0],
					ctx.Args[1],
				},
			}
			aofEngine.Write(ToRespBytes(cmd))
		}
		return protocol.Value{Type: protocol.Integer, Num: 1}
	}
	return protocol.Value{Type: protocol.Integer, Num: 0}
}

// 以秒为单位返回剩余时间
func TTL(ctx *core.Context) protocol.Value {
	return ttlGeneric(ctx, time.Second)
}

// 以毫秒为单位返回剩余时间
func PTTL(ctx *core.Context) protocol.Value {
	return ttlGeneric(ctx, time.Millisecond)
}

// TTL处理
func ttlGeneric(ctx *core.Context, unit time.Duration) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for command"}
	}

	key := string(ctx.Args[0].Bulk)

	duration, found, _ := ctx.DB.GetTTL(key)

	if !found {
		return protocol.Value{Type: protocol.Integer, Num: -2}
	}

	if duration == 0 {
		return protocol.Value{Type: protocol.Integer, Num: -1}
	}

	// 转换时间单位
	var t int64
	if unit == time.Second {
		t = int64(duration.Seconds())
	} else {
		t = int64(duration.Milliseconds())
	}

	if t < 0 {
		t = 0
	}

	return protocol.Value{Type: protocol.Integer, Num: t}
}

// 移除过期时间
func Persist(ctx *core.Context) protocol.Value {
	if len(ctx.Args) != 1 {
		return protocol.Value{Type: protocol.Error, Str: "ERR wrong number of arguments for 'persist' command"}
	}

	key := string(ctx.Args[0].Bulk)

	removed, _ := ctx.DB.RmExpiration(key)

	if removed {
		if aofEngine := ctx.DB.GetAof(); aofEngine != nil {
			cmd := protocol.Value{
				Type: protocol.Array,
				Array: []protocol.Value{
					{Type: protocol.BulkString, Bulk: []byte("PERSIST")},
					ctx.Args[0],
				},
			}
			aofEngine.Write(ToRespBytes(cmd))
		}
		return protocol.Value{Type: protocol.Integer, Num: 1}
	}

	return protocol.Value{Type: protocol.Integer, Num: 0}
}
