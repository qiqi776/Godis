package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// 定义RESP类型前缀
const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

// 表示一个解析后的 Redis 值
type Value struct {
	Type   byte
	Str   string
	Num   int64
	Bulk  []byte
	Array []Value
}

// 序列化回复
type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes []byte

	switch v.Type {
	case SimpleString:
		bytes = []byte(fmt.Sprintf("+%s\r\n", v.Str))
	case Error:
		bytes = []byte(fmt.Sprintf("-%s\r\n", v.Str))
	case Integer:
		bytes = []byte(fmt.Sprintf(":%d\r\n", v.Num))
	case BulkString:
		if v.Bulk == nil {
			bytes = []byte("$-1\r\n")
		} else {
			bytes = []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v.Bulk), v.Bulk))
		}
	case Array:
		bytes = []byte(fmt.Sprintf("*%d\r\n", len(v.Array)))
		// 先写数组头
		if _, err := w.writer.Write(bytes); err != nil {
			return err
		}
		// 再递归写元素
		for _, val := range v.Array {
			if err := w.Write(val); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown type: %v", v.Type)
	}

	_, err := w.writer.Write(bytes)
	return err
}

// 用于从网络流中解析 RESP
type Reader struct {
	reader *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{reader: bufio.NewReader(rd)}
}

// 读取一个完整的 RESP 对象
func (r *Reader) ReadValue() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case Array:
		return r.readArray()
	case BulkString:
		return r.readBulk()
	// 暂时为了服务端只需实现这两个用于读取客户端请求
	default:
		return Value{}, fmt.Errorf("unknown type: %v", string(_type))
	}
}

func (r *Reader) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

func (r *Reader) readInteger() (int64, error) {
	line, _, err := r.readLine()
	if err != nil {
		return 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, err
	}
	return i64, nil
}

func (r *Reader) readBulk() (Value, error) {
	val := Value{Type: BulkString}

	// 读取长度
	len, err := r.readInteger()
	if err != nil {
		return val, err
	}

	if len == -1 {
		val.Bulk = nil
		return val, nil
	}

	// 读取实际数据
	bulk := make([]byte, len)
	_, err = io.ReadFull(r.reader, bulk)
	if err != nil {
		return val, err
	}

	// 读取末尾的 \r\n
	_, _, err = r.readLine()
	if err != nil {
		return val, err
	}

	val.Bulk = bulk
	return val, nil
}

func (r *Reader) readArray() (Value, error) {
	val := Value{Type: Array}

	// 读取数组个数
	len, err := r.readInteger()
	if err != nil {
		return val, err
	}

	val.Array = make([]Value, 0)
	for i := 0; i < int(len); i++ {
		elem, err := r.ReadValue()
		if err != nil {
			return val, err
		}
		val.Array = append(val.Array, elem)
	}

	return val, nil
}