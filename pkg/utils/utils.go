package utils

// ToCmdLine 将一组字符串转换为 Redis 命令行格式 ([][]byte)
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

// ToCmdLine2 将命令名称和字符串参数转换为 Redis 命令行格式
func ToCmdLine2(commandName string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = []byte(s)
	}
	return result
}

// ToCmdLine3 将命令名称和字节切片参数转换为 Redis 命令行格式
func ToCmdLine3(commandName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = s
	}
	return result
}

// Equals 比较两个接口是否相等
func Equals(a interface{}, b interface{}) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}

// BytesEquals 比较两个字节切片的内容是否完全一致
func BytesEquals(a []byte, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}

// ConvertRange 将 Redis 风格的索引范围转换为 Go 切片风格的索引范围
func ConvertRange(start int64, end int64, size int64) (int, int) {
	if start < -size {
		return -1, -1
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return -1, -1
	}
	if end < -size {
		return -1, -1
	} else if end < 0 {
		end = size + end + 1
	} else if end < size {
		end = end + 1
	} else {
		end = size
	}
	if start > end {
		return -1, -1
	}
	return int(start), int(end)
}

// RemoveDuplicates 对二维字节切片进行去重
func RemoveDuplicates(input [][]byte) [][]byte {
	uniqueMap := make(map[string]struct{})
	var result [][]byte
	for _, item := range input {
		key := string(item)
		if _, exists := uniqueMap[key]; !exists {
			uniqueMap[key] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}