package wildcard

import (
	"errors"
	"regexp"
	"strings"
)

// Pattern 表示一个已经编译完成的通配符模式（底层转换为正则表达式）
type Pattern struct {
	exp *regexp.Regexp
}

// replaceMap 负责将通配符语法转换为等价的正则表达式片段
var replaceMap = map[byte]string{
	'+': `\+`,
	'(': `\(`,
	')': `\)`,
	'$': `\$`,
	'.': `\.`,
	'{': `\{`,
	'}': `\}`,
	'|': `\|`,
	'*': ".*",
	'?': ".",
}

var errEndWithEscape = "end with escape \\"

// CompilePattern 将通配符字符串编译为 Pattern
func CompilePattern(src string) (*Pattern, error) {
	regexSrc := strings.Builder{}
	regexSrc.WriteByte('^')
	for i := 0; i < len(src); i++ {
		ch := src[i]
		if ch == '\\' {
			if i == len(src)-1 {
				return nil, errors.New(errEndWithEscape)
			}
			regexSrc.WriteByte(ch)
			regexSrc.WriteByte(src[i+1])
			i++ // skip escaped character
		} else if ch == '^' {
			if i == 0 {
				regexSrc.WriteString(`\^`)
			} else if i == 1 {
				if src[i-1] == '[' {
					regexSrc.WriteString(`^`) // src is: [^
				} else {
					regexSrc.WriteString(`\^`)
				}
			} else {
				if src[i-1] == '[' && src[i-2] != '\\' {
					regexSrc.WriteString(`^`) // src is: [^, except \[^
				} else {
					regexSrc.WriteString(`\^`)
				}
			}
		} else if escaped, toEscape := replaceMap[ch]; toEscape {
			regexSrc.WriteString(escaped)
		} else {
			regexSrc.WriteByte(ch)
		}
	}
	regexSrc.WriteByte('$')
	re, err := regexp.Compile(regexSrc.String())
	if err != nil {
		return nil, err
	}
	return &Pattern{
		exp: re,
	}, nil
}

// IsMatch 判断目标字符串是否满足当前 Pattern
func (p *Pattern) IsMatch(s string) bool {
	return p.exp.Match([]byte(s))
}
