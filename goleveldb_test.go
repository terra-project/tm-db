package db

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestGoLevelDBNewGoLevelDB(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)

	// Test we can't open the db twice for writing
	wr1, err := NewGoLevelDB(name, "")
	require.Nil(t, err)
	_, err = NewGoLevelDB(name, "")
	require.NotNil(t, err)
	wr1.Close() // Close the db to release the lock

	// Test we can open the db twice for reading only
	ro1, err := NewGoLevelDBWithOpts(name, "", &opt.Options{ReadOnly: true})
	require.Nil(t, err)
	defer ro1.Close()
	ro2, err := NewGoLevelDBWithOpts(name, "", &opt.Options{ReadOnly: true})
	require.Nil(t, err)
	defer ro2.Close()
}

func BenchmarkGoLevelDBRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := NewGoLevelDB(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		db.Close()
		cleanupDBDir("", name)
	}()

	benchmarkRandomReadsWrites(b, db)
}

func BenchmarkGoLevelDBRandomReads(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := NewGoLevelDB(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		db.Close()
		cleanupDBDir("", name)
	}()

	benchmarkGoLevelDBRandomReads(b, db, 100000)
}

func BenchmarkGoLevelDBRangeScans(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := NewGoLevelDB(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		db.Close()
		cleanupDBDir("", name)
	}()

	benchmarkGoLevelDBRangeScans(b, db, 100000, 100)
}

func benchmarkGoLevelDBRandomReads(b *testing.B, db DB, numItems int64) {
	for i := 0; i < int(numItems); i++ {
		item := int64(i)
		err := db.Set(int642Bytes(item), int642Bytes(item))
		if err != nil {
			b.Fatal(b, err)
		}
	}

	b.StartTimer()
	for j := 0; j < b.N; j++ {
		idx := rand.Int63n(numItems)
		val, err := db.Get(int642Bytes(idx))
		require.NoError(b, err)
		require.Equal(b, idx, bytes2Int64(val))
	}
}

func benchmarkGoLevelDBRangeScans(b *testing.B, db DB, numItems int64, rangeSize int64) {
	for i := 0; i < int(numItems); i++ {
		item := int64(i)
		err := db.Set(int642Bytes(item), int642Bytes(item))
		if err != nil {
			b.Fatal(b, err)
		}
	}

	b.StartTimer()
	for j := 0; j < b.N; j++ {

		start := rand.Int63n(numItems - rangeSize)
		end := start + rangeSize
		iter, err := db.Iterator(int642Bytes(start), int642Bytes(end))
		require.NoError(b, err)
		count := 0
		for ; iter.Valid(); iter.Next() {
			count++
		}
		iter.Close()
		require.EqualValues(b, rangeSize, count)
	}
}
