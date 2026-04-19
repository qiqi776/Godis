package dict

type Dict struct {
	data map[string]any
}

func New() *Dict {
	return &Dict{
		data: make(map[string]any),
	}
}

func (d *Dict) Get(key string) (any, bool) {
	value, ok := d.data[key]
	return value, ok
}

func (d *Dict) Put(key string, value any) int64 {
	_, exists := d.data[key]
	d.data[key] = value
	if exists {
		return 0
	}
	return 1
}

func (d *Dict) Remove(key string) (any, int64) {
	value, exists := d.data[key]
	if !exists {
		return nil, 0
	}
	delete(d.data, key)
	return value, 1
}

func (d *Dict) Len() int {
	return len(d.data)
}

func (d *Dict) Keys() []string {
	keys := make([]string, 0, len(d.data))
	for key := range d.data {
		keys = append(keys, key)
	}
	return keys
}

func (d *Dict) ForEach(consumer func(key string, value any) bool) {
	for key, value := range d.data {
		if !consumer(key, value) {
			break
		}
	}
}

func (d *Dict) Clear() {
	d.data = make(map[string]any)
}
