package list

import "container/list"

const pageSize = 1024

type QuickList struct {
	data *list.List
	size  int
}

type iterator struct {
	node  *list.Element
	offset int
	ql    *QuickList
}

func MakeQuickList() *QuickList {
	ql := &QuickList{
		data: list.New(),
	}
	return ql
}