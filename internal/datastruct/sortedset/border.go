package sortedset

import (
	"errors"
	"strconv"
)

const (
	scoreNegativeInf int8 = -1
	scorePositiveInf int8 = 1
	lexNegativeInf   int8 = '-'
	lexPositiveInf   int8 = '+'
)

type Border interface {
	greater(element *Element) bool
	less(element *Element) bool
	getValue() interface{}
	getExclude() bool
	isIntersected(max Border) bool
}

type ScoreBorder struct {
	Inf 	int8
	Value   float64
	Exclude bool
}

// 判断是否大于
func (border *ScoreBorder) greater(element *Element) bool {
	value := element.Score
	if border.Inf == scoreNegativeInf {
		return false
	} else if border.Inf == scorePositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

// 判断是否小于
func (border *ScoreBorder) less(element *Element) bool {
	value := element.Score
	if border.Inf == scoreNegativeInf {
		return true
	} else if border.Inf == scorePositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

// 获取值
func (border *ScoreBorder) getValue() interface{} {
	return border.Value
}

// 边界是否包含
func (border *ScoreBorder) getExclude() bool {
	return border.Exclude
}

// 判断range是否无效
func (border *ScoreBorder) isIntersected(max Border) bool {
	maxBorder := max.(*ScoreBorder)
	if border.Inf != maxBorder.Inf {
		return border.Inf > maxBorder.Inf
	}
	if border.Inf != 0 {
		return border.Exclude || maxBorder.Exclude
	}
	minValue := border.Value
	maxValue := max.(*ScoreBorder).Value
	return minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}

// 正负无穷
var scorePositiveInfBorder = &ScoreBorder{Inf: scorePositiveInf}
var scoreNegativeInfBorder = &ScoreBorder{Inf: scoreNegativeInf}


// 解析对象
func ParseScoreBorder(s string) (Border, error) {
	// 处理正无穷和负无穷
	if s == "inf" || s == "+inf" {
		return scorePositiveInfBorder, nil
	}
	if s == "-inf" {
		return scoreNegativeInfBorder, nil
	}
	if len(s) == 0 {
        return nil, errors.New("ERR empty string")
    }
    if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, errors.New("ERR min or max is not a float")
		}
		return &ScoreBorder{
			Inf:	 0,
			Value:   value,
			Exclude: true,
		}, nil
	}
    value, err := strconv.ParseFloat(s, 64)
    if err != nil {
        return nil, errors.New("ERR min or max is not a float")
    }
    return &ScoreBorder{
        Inf:     0,   // 不是无穷大
        Value:   value,
        Exclude: false, // 默认包含边界
    }, nil
}

type LexBorder struct {
	Inf 	int8
	Value   string
	Exclude bool
}

// 判断是否大于
func (border *LexBorder) greater(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return false
	} else if border.Inf == lexPositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

// 判断是否小于
func (border *LexBorder) less(element *Element) bool {
	value := element.Member
	if border.Inf == lexNegativeInf {
		return true
	} else if border.Inf == lexPositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

// 获取值
func (border *LexBorder) getValue() interface{} {
	return border.Value
}

// 边界是否包含
func (border *LexBorder) getExclude() bool {
	return border.Exclude
}

// 判断range是否无效
func (border *LexBorder) isIntersected(max Border) bool {
	maxBorder := max.(*LexBorder)
	if border.Inf == lexPositiveInf {
		return true
	}
	if maxBorder.Inf == lexNegativeInf {
		return true
	}
	if border.Inf == lexNegativeInf || maxBorder.Inf == lexPositiveInf {
		return false
	}
	minValue := border.Value
	maxValue := max.(*LexBorder).Value
	return minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}

var lexPositiveInfBorder = &LexBorder{Inf: lexPositiveInf}
var lexNegativeInfBorder = &LexBorder{Inf: lexNegativeInf}

// 解析对象
func ParseLexBorder(s string) (Border, error) {
	if s == "+" {
		return lexPositiveInfBorder, nil
	}
	if s == "-" {
		return lexNegativeInfBorder, nil
	}
	if len(s) == 0 {
        return nil, errors.New("ERR min or max not valid string range item")
    }
	if s[0] == '(' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: true,
		}, nil
	}
	if s[0] == '[' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: false,
		}, nil
	}

	return nil, errors.New("ERR min or max not valid string range item")
}