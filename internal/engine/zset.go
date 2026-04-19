package engine

import "godis/internal/datastruct/zset"

func (db *DB) ZAdd(key string, score float64, member string) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	zs, ok, err := getValue[*zset.ZSet](db, key, KindZSet)
	if err != nil {
		return 0, err
	}
	if !ok {
		zs = zset.New()
		db.setValue(key, KindZSet, zs)
	}

	return zs.Add(member, score), nil
}

func (db *DB) ZRem(key string, members ...string) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	zs, ok, err := getValue[*zset.ZSet](db, key, KindZSet)
	if err != nil || !ok {
		return 0, err
	}

	removed := zs.Remove(members...)
	if zs.Len() == 0 {
		db.deleteKey(key)
	}
	return removed, nil
}

func (db *DB) ZScore(key, member string) (float64, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	zs, ok, err := getValue[*zset.ZSet](db, key, KindZSet)
	if err != nil || !ok {
		return 0, false, err
	}

	score, ok := zs.Score(member)
	return score, ok, nil
}

func (db *DB) ZRange(key string, start, stop int64) ([][]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	zs, ok, err := getValue[*zset.ZSet](db, key, KindZSet)
	if err != nil {
		return nil, err
	}
	if !ok {
		return [][]byte{}, nil
	}

	from, to, ok := normalizeRange(zs.Len(), start, stop)
	if !ok {
		return [][]byte{}, nil
	}

	elements := zs.Range(from, to)
	out := make([][]byte, 0, len(elements))
	for _, e := range elements {
		out = append(out, []byte(e.Member))
	}
	return out, nil
}
