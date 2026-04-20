package engine

import "godis/internal/datastruct/bitmap"

func (db *DB) SetBit(key string, offset int64, bit int) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	bm, ok, err := getValue[*bitmap.Bitmap](db, key, KindBitmap)
	if err != nil {
		return 0, err
	}
	if !ok {
		bm = bitmap.New()
		db.setValue(key, KindBitmap, bm)
	}

	old, err := bm.SetBit(offset, bit)
	if err != nil && ok {
		db.touchKey(key)
	}
	return old, nil
}

func (db *DB) GetBit(key string, offset int64) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	bm, ok, err := getValue[*bitmap.Bitmap](db, key, KindBitmap)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}

	return bm.GetBit(offset)
}

func (db *DB) BitCount(key string) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	bm, ok, err := getValue[*bitmap.Bitmap](db, key, KindBitmap)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}

	return bm.Count(), nil
}
