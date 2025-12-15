package core

import "strconv"

const (
	ObjectTypeString = iota
	ObjectTypeList
	ObjectTypeHash
	ObjectTypeSet
	ObjectTypeZSet
)

type RedisObject struct {
	Type     int
	Encoding int
	Ptr      interface{}
}

// 辅助方法, 将interface{}转为int64
func (obj *RedisObject) AsInt64() (int64, bool) {
	if obj.Type != ObjectTypeString {
		return 0, false
	}
	valStr, ok := obj.Ptr.(string)
	if !ok {
		return 0, false
	}
	val, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}