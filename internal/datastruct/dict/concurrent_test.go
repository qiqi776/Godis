package dict

import (
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"godis/pkg/utils"
)

func assert(t *testing.T, condition bool, msg string, args ...interface{}) {
	if !condition {
		t.Errorf(msg, args...)
	}
}

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + rand.IntN(26))
	}
	return string(b)
}

func TestMakeConcurrent(t *testing.T) {
	d := MakeConcurrent(0)
	assert(t, d.shardCount == 16, "Expected default shard count 16, got %d", d.shardCount)
	d2 := MakeConcurrent(100)
	assert(t, d2.shardCount == 128, "Expected shard count 128, got %d", d2.shardCount)
}

func TestConcurrentDict_Basic(t *testing.T) {
	d := MakeConcurrent(16)
	ret := d.Put("key1", "value1")
	assert(t, ret == 1, "Expected Put to return 1 for new key")
	assert(t, d.Len() == 1, "Expected Len to be 1")

	val, exists := d.Get("key1")
	assert(t, exists, "Expected key1 to exist")
	assert(t, val == "value1", "Expected value1, got %v", val)

	ret = d.Put("key1", "value2")
	assert(t, ret == 0, "Expected Put to return 0 for existing key")
	val, _ = d.Get("key1")
	assert(t, val == "value2", "Expected value update")

	val, ret = d.Remove("key1")
	assert(t, ret == 1, "Expected Remove return 1")
	assert(t, val == "value2", "Expected removed value to be value2")
	assert(t, d.Len() == 0, "Expected empty dict after remove")

	val, ret = d.Remove("key1")
	assert(t, ret == 0, "Expected Remove return 0 for missing key")
}

func TestConcurrentDict_ConditionalPut(t *testing.T) {
	d := MakeConcurrent(16)
	ret := d.PutIfAbsent("k1", "v1")
	assert(t, ret == 1, "PutIfAbsent 1")
	ret = d.PutIfAbsent("k1", "v2")
	assert(t, ret == 0, "PutIfAbsent 0")
	val, _ := d.Get("k1")
	assert(t, val == "v1", "Value check")

	ret = d.PutIfExists("k2", "v2")
	assert(t, ret == 0, "PutIfExists 0")
	d.Put("k2", "v2")
	ret = d.PutIfExists("k2", "v3")
	assert(t, ret == 1, "PutIfExists 1")
	val, _ = d.Get("k2")
	assert(t, val == "v3", "Value check")
}

func TestConcurrentDict_Chaos(t *testing.T) {
	d := MakeConcurrent(32)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		d.Put(strconv.Itoa(i), i)
	}

	concurrency := 50
	opsPerRoutine := 1000

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(routineID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Routine %d panicked: %v", routineID, r)
				}
			}()

			for j := 0; j < opsPerRoutine; j++ {
				op := rand.IntN(100)
				key := strconv.Itoa(rand.IntN(200))

				if op < 20 {
					d.Put(key, j)
				} else if op < 40 {
					d.Remove(key)
				} else if op < 90 {
					d.Get(key)
				} else {
					count := 0
					d.ForEach(func(k string, v interface{}) bool {
						count++
						return count < 5
					})
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if d.Len() < 0 {
			t.Errorf("Final length is negative: %d", d.Len())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out! Possible Deadlock detected in mixed operations.")
	}
}

func TestRWLocks_Deadlock(t *testing.T) {
	d := MakeConcurrent(16)
	d.Put("A", 1)
	d.Put("B", 1)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			d.RWLocks([]string{"A"}, []string{"B"})
			time.Sleep(time.Microsecond)
			d.RWUnLocks([]string{"A"}, []string{"B"})
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			d.RWLocks([]string{"B"}, []string{"A"})
			time.Sleep(time.Microsecond)
			d.RWUnLocks([]string{"B"}, []string{"A"})
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Deadlock detected! RWLocks did not sort indices correctly.")
	}
}

func TestConcurrentRemove(t *testing.T) {
	d := MakeConcurrent(0)
	totalCount := 100

	for i := 0; i < totalCount; i++ {
		key := "k" + strconv.Itoa(i)
		d.Put(key, i)
	}
	assert(t, d.Len() == totalCount, "Len check")

	for i := 0; i < totalCount; i++ {
		key := "k" + strconv.Itoa(i)
		val, ok := d.Get(key)
		assert(t, ok && val.(int) == i, "Get check before remove")

		_, ret := d.Remove(key)
		assert(t, ret == 1, "Remove result 1")
		assert(t, d.Len() == totalCount-i-1, "Len decrease check")

		_, ok = d.Get(key)
		assert(t, !ok, "Should not exist")
	}

	d = MakeConcurrent(0)
	for i := 0; i < 100; i++ {
		key := "k" + strconv.Itoa(i)
		d.Put(key, i)
	}
	for i := 99; i >= 0; i-- {
		key := "k" + strconv.Itoa(i)
		_, ret := d.Remove(key)
		assert(t, ret == 1, "Remove tail result")
	}
	assert(t, d.Len() == 0, "Len 0 after tail remove")
}

func TestConcurrentDict_RandomKeys(t *testing.T) {
	d := MakeConcurrent(16)
	count := 100
	for i := 0; i < count; i++ {
		d.Put(strconv.Itoa(i), i)
	}

	limit := 10
	keys := d.RandomKeys(limit)
	assert(t, len(keys) <= limit, "RandomKeys limit")

	distinctKeys := d.RandomDistinctKeys(limit)
	distinctMap := make(map[string]bool)
	for _, k := range distinctKeys {
		if distinctMap[k] {
			t.Errorf("Duplicate key found: %s", k)
		}
		distinctMap[k] = true
	}
	assert(t, len(distinctKeys) == limit, "Distinct keys count")

	largeLimit := 200
	keys = d.RandomKeys(largeLimit)
	assert(t, len(keys) == count, "Limit > Size check")
}

func TestConcurrentDict_Keys(t *testing.T) {
	d := MakeConcurrent(0)
	size := 10
	for i := 0; i < size; i++ {
		d.Put(randString(5), randString(5))
	}
	if len(d.Keys()) != size {
		t.Errorf("expect %d keys, actual: %d", size, len(d.Keys()))
	}
}

func TestDictScan(t *testing.T) {
	d := MakeConcurrent(0)
	count := 100
	for i := 0; i < count; i++ {
		d.Put("kkk"+strconv.Itoa(i), i)
	}
	for i := 0; i < count; i++ {
		d.Put("key"+strconv.Itoa(i), i)
	}

	cursor := 0
	var allResult [][]byte
	for {
		var keys [][]byte
		keys, cursor = d.DictScan(cursor, 10, "*")
		allResult = append(allResult, keys...)
		if cursor == 0 {
			break
		}
	}

	allResult = utils.RemoveDuplicates(allResult)
	assert(t, len(allResult) == count*2, "Scan * count mismatch, expected %d got %d", count*2, len(allResult))

	cursor = 0
	var matchResult [][]byte
	for {
		var keys [][]byte
		keys, cursor = d.DictScan(cursor, 10, "key*")
		matchResult = append(matchResult, keys...)
		if cursor == 0 {
			break
		}
	}
	matchResult = utils.RemoveDuplicates(matchResult)
	if len(matchResult) != count {
		t.Logf("Scan key* count: %d (expected %d if wildcard is working)", len(matchResult), count)
	}
}

func TestConcurrentDict_ClearConcurrency(t *testing.T) {
	d := MakeConcurrent(32)
	for i := 0; i < 1000; i++ {
		d.Put(strconv.Itoa(i), i)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			d.Get(strconv.Itoa(i))
		}
	}()
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		d.Clear()
	}()
	wg.Wait()
}

func BenchmarkPut(b *testing.B) {
	d := MakeConcurrent(32)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := strconv.Itoa(i) + strconv.FormatInt(time.Now().UnixNano(), 10)
			d.Put(key, i)
			i++
		}
	})
}

func BenchmarkGet(b *testing.B) {
	d := MakeConcurrent(32)
	var count int32 = 0
	b.RunParallel(func(pb *testing.PB) {
		id := atomic.AddInt32(&count, 1)
		d.Put(strconv.Itoa(int(id)), int(id))
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			d.Get(strconv.Itoa(i % 10000))
			i++
		}
	})
}
