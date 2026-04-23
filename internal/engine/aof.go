package engine

import (
	"math"
	"sort"
	"strconv"
	"time"

	"godis/internal/datastruct/bitmap"
	"godis/internal/datastruct/hash"
	"godis/internal/datastruct/list"
	"godis/internal/datastruct/set"
	"godis/internal/datastruct/zset"
)

func (e *Engine) AOFRewriteCommands() [][][]byte {
	commands := make([][][]byte, 0)
	for dbIndex, db := range e.dbs {
		dbCommands := db.aofRewriteCommands()
		if len(dbCommands) == 0 {
			continue
		}

		commands = append(commands, [][]byte{
			[]byte("SELECT"),
			[]byte(strconv.Itoa(dbIndex)),
		})
		commands = append(commands, dbCommands...)
	}
	return commands
}

func (db *DB) aofRewriteCommands() [][][]byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	keys := make([]string, 0, len(db.data))
	for key := range db.data {
		if db.isExpired(key) {
			db.deleteKey(key)
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)

	commands := make([][][]byte, 0, len(keys))
	for _, key := range keys {
		var expireSeconds int64
		if expireAt, ok := db.expireAt[key]; ok {
			expireSeconds = int64(math.Ceil(time.Until(expireAt).Seconds()))
			if expireSeconds <= 0 {
				db.deleteKey(key)
				continue
			}
		}

		entity := db.data[key]
		commands = append(commands, rewriteEntityCommands(key, entity)...)

		if expireSeconds > 0 {
			commands = append(commands, [][]byte{
				[]byte("EXPIRE"),
				[]byte(key),
				[]byte(strconv.FormatInt(expireSeconds, 10)),
			})
		}
	}
	return commands
}

func rewriteEntityCommands(key string, entity *Entity) [][][]byte {
	switch entity.Kind {
	case KindString:
		value, _ := entity.Value.([]byte)
		return [][][]byte{{
			[]byte("SET"),
			[]byte(key),
			copyBytes(value),
		}}
	case KindList:
		lst, _ := entity.Value.(*list.List)
		if lst == nil || lst.Len() == 0 {
			return nil
		}
		values := lst.Range(0, lst.Len()-1)
		cmd := make([][]byte, 0, len(values)+2)
		cmd = append(cmd, []byte("RPUSH"), []byte(key))
		cmd = append(cmd, values...)
		return [][][]byte{cmd}
	case KindHash:
		h, _ := entity.Value.(*hash.Hash)
		if h == nil || h.Len() == 0 {
			return nil
		}
		values := h.GetAll()
		commands := make([][][]byte, 0, len(values)/2)
		for i := 0; i+1 < len(values); i += 2 {
			commands = append(commands, [][]byte{
				[]byte("HSET"),
				[]byte(key),
				values[i],
				values[i+1],
			})
		}
		return commands
	case KindSet:
		st, _ := entity.Value.(*set.Set)
		if st == nil || st.Len() == 0 {
			return nil
		}
		members := st.Members()
		cmd := make([][]byte, 0, len(members)+2)
		cmd = append(cmd, []byte("SADD"), []byte(key))
		cmd = append(cmd, members...)
		return [][][]byte{cmd}
	case KindZSet:
		zs, _ := entity.Value.(*zset.ZSet)
		if zs == nil || zs.Len() == 0 {
			return nil
		}
		elements := zs.Range(0, zs.Len()-1)
		commands := make([][][]byte, 0, len(elements))
		for _, element := range elements {
			commands = append(commands, [][]byte{
				[]byte("ZADD"),
				[]byte(key),
				[]byte(strconv.FormatFloat(element.Score, 'f', -1, 64)),
				[]byte(element.Member),
			})
		}
		return commands
	case KindBitmap:
		bm, _ := entity.Value.(*bitmap.Bitmap)
		if bm == nil || bm.Count() == 0 {
			return nil
		}
		bits := bm.SetBits()
		commands := make([][][]byte, 0, len(bits))
		for _, offset := range bits {
			commands = append(commands, [][]byte{
				[]byte("SETBIT"),
				[]byte(key),
				[]byte(strconv.FormatInt(offset, 10)),
				[]byte("1"),
			})
		}
		return commands
	default:
		return nil
	}
}
