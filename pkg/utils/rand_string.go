package utils

import (
	"math/rand/v2"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

// RandString 生成长度为 n 的随机字符串
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return string(b)
}

var hexLetters = []rune("0123456789abcdef")

// RandHexString 生成长度为 n 的随机十六进制字符串
func RandHexString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = hexLetters[rand.IntN(len(hexLetters))]
	}
	return string(b)
}

// RandIndex 生成随机索引序列
func RandIndex(size int) []int {
	result := make([]int, size)
	for i := range result {
		result[i] = i
	}
	rand.Shuffle(size, func(i, j int) {
		result[i], result[j] = result[j], result[i]
	})
	return result
}
