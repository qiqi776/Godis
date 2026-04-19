package engine

import "godis/internal/datastruct/set"

func (db *DB) SAdd(key string, members ...string) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	st, ok, err := getValue[*set.Set](db, key, KindSet)
	if err != nil {
		return 0, err
	}
	if !ok {
		st = set.New()
		db.setValue(key, KindSet, st)
	}

	return st.Add(members...), nil
}

func (db *DB) SRem(key string, members ...string) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	st, ok, err := getValue[*set.Set](db, key, KindSet)
	if err != nil || !ok {
		return 0, err
	}

	removed := st.Remove(members...)
	if st.Len() == 0 {
		db.deleteKey(key)
	}
	return removed, nil
}

func (db *DB) SMembers(key string) ([][]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	st, ok, err := getValue[*set.Set](db, key, KindSet)
	if err != nil {
		return nil, err
	}
	if !ok {
		return [][]byte{}, nil
	}

	return st.Members(), nil
}

func (db *DB) SIsMember(key, member string) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	st, ok, err := getValue[*set.Set](db, key, KindSet)
	if err != nil || !ok {
		return false, err
	}

	return st.Has(member), nil
}
