package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBCache(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)

	db, err := NewDBCache(name, "")
	require.Nil(t, err)

	// directly to db
	db.db.Set([]byte("key1"), []byte("value"))
	v, _ := db.Get([]byte("key1"))
	require.NotNil(t, v)

	// to cache
	db.cache.Set([]byte("key2"), []byte("value"))
	v2, _ := db.Get([]byte("key2"))
	require.NotNil(t, v2)

	db.Close()
}

func TestDBCacheIterator(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)

	db, err := NewDBCache(name, "")
	require.Nil(t, err)

	// to cache
	db.cache.Set([]byte("key1"), []byte("value1"))
	db.cache.Set([]byte("key2"), []byte("value2"))
	db.cache.Set([]byte("key3"), []byte("value3"))
	// commit cache
	db.commitCache.Set([]byte("key4"), []byte("value4"))
	db.commitCache.Set([]byte("key5"), []byte("value5"))
	db.commitCache.Set([]byte("key6"), []byte("value6"))
	// to db
	db.db.Set([]byte("key7"), []byte("value7"))
	db.db.Set([]byte("key8"), []byte("value8"))
	db.db.Set([]byte("key9"), []byte("value9"))

	itr, err := db.Iterator([]byte("key1"), []byte("key9"))

	for i := 1; itr.Valid(); itr.Next() {
		assert.Equal(t, fmt.Sprintf("key%d", i), string(itr.Key()))
		i++
	}

	db.Close()
}

func TestDBCacheReverseIterator(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)

	db, err := NewDBCache(name, "")
	require.Nil(t, err)

	// to cache
	db.cache.Set([]byte("key1"), []byte("value1"))
	db.cache.Set([]byte("key2"), []byte("value2"))
	db.cache.Set([]byte("key3"), []byte("value3"))
	// commit cache
	db.commitCache.Set([]byte("key4"), []byte("value4"))
	db.commitCache.Set([]byte("key5"), []byte("value5"))
	db.commitCache.Set([]byte("key6"), []byte("value6"))
	// to db
	db.db.Set([]byte("key7"), []byte("value4"))
	db.db.Set([]byte("key8"), []byte("value5"))
	db.db.Set([]byte("key9"), []byte("value6"))

	itr, err := db.ReverseIterator([]byte("key1"), []byte("key9"))

	for i := 8; itr.Valid(); itr.Next() {
		assert.Equal(t, fmt.Sprintf("key%d", i), string(itr.Key()))
		i--
	}

	db.Close()
}

func TestDBCacheIteratorWithFlush(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)

	db, err := NewDBCache(name, "")
	require.Nil(t, err)

	db.Set([]byte("key4"), []byte("value4"))
	db.Set([]byte("key5"), []byte("value5"))
	db.Set([]byte("key6"), []byte("value6"))
	db.Close()
	db.Set([]byte("key2"), []byte("value1"))
	db.Set([]byte("key3"), []byte("value2"))
	db.Close()
	db.Set([]byte("key1"), []byte("value3"))
	itr, err := db.Iterator([]byte("key1"), []byte("key7"))

	for i := 1; itr.Valid(); itr.Next() {
		assert.Equal(t, fmt.Sprintf("key%d", i), string(itr.Key()))
		i++
	}
	db.Close()
}

func TestDBCacheReverseIteratorWithFlush(t *testing.T) {
	name := fmt.Sprintf("test_%x", randStr(12))
	defer cleanupDBDir("", name)

	db, err := NewDBCache(name, "")
	require.Nil(t, err)

	db.Set([]byte("key4"), []byte("value4"))
	db.Set([]byte("key5"), []byte("value5"))
	db.Set([]byte("key6"), []byte("value6"))
	db.Close()
	db.Set([]byte("key2"), []byte("value1"))
	db.Set([]byte("key3"), []byte("value2"))
	db.Close()
	db.Set([]byte("key1"), []byte("value3"))

	itr, err := db.ReverseIterator([]byte("key1"), []byte("key7"))

	for i := 6; itr.Valid(); itr.Next() {
		assert.Equal(t, fmt.Sprintf("key%d", i), string(itr.Key()))
		i--
	}

	db.Close()
}

func BenchmarkDBCacheRandomReadsWrites(b *testing.B) {
	name := fmt.Sprintf("test_%x", randStr(12))
	db, err := NewDBCache(name, "")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		db.Close()
		cleanupDBDir("", name)
	}()

	benchmarkRandomReadsWrites(b, db)
}
