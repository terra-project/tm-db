package db

import (
	"bytes"
	"fmt"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewDBCache(name, dir)
	}
	registerDBCreator(DBCacheBackend, dbCreator, false)
}

type DBCache struct {
	db            *GoLevelDB
	name          string
	cache         *MemDB
	commitCache   *MemDB // height - 1
	currentBatch  Batch
	commitBatches []Batch
	persistentKey []byte
}

func NewDBCache(name string, dir string) (*DBCache, error) {
	database, err := NewGoLevelDB(name, dir)
	if err != nil {
		return nil, err
	}
	cache := NewMemDB()
	commitCache := NewMemDB()
	// validators should always be writtent directly to persistant memory
	persistentKey := []byte("validatorsKey")
	dbcache := &DBCache{
		db:            database,
		name:          name,
		cache:         cache,
		commitCache:   commitCache,
		currentBatch:  newGoLevelDBBatch(database),
		persistentKey: persistentKey,
	}
	return dbcache, nil
}

// Get implements DB.
func (db *DBCache) Get(key []byte) ([]byte, error) {
	var v []byte
	v, _ = db.cache.Get(key)
	if v != nil {
		// cache hit
		return v, nil
	}

	v, _ = db.commitCache.Get(key)
	if v != nil {
		// commit cache hit
		return v, nil
	}

	// cache miss
	v, err := db.db.Get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// Has implements DB.
func (db *DBCache) Has(key []byte) (bool, error) {
	okCache, _ := db.cache.Has(key)
	if okCache {
		// cache hit
		return true, nil
	}

	okCommitCache, _ := db.commitCache.Has(key)
	if okCommitCache {
		// commit cache hit
		return true, nil
	}

	// cache miss
	okDb, _ := db.db.Has(key)
	if okDb {
		return true, nil
	}
	return false, nil
}

// Set implements DB.
func (db *DBCache) Set(key []byte, value []byte) error {
	if db.isPersistentKey(key) {
		db.ForceSet(key, value)
	}
	db.cache.Set(key, value)
	db.currentBatch.Set(key, value)
	return nil
}

// SetSync implements DB.
func (db *DBCache) SetSync(key []byte, value []byte) error {
	return db.Set(key, value)
}

// Write directly to persistent memory
func (db *DBCache) ForceSet(key []byte, value []byte) error {
	return db.db.SetSync(key, value)
}

// Delete implements DB.
func (db *DBCache) Delete(key []byte) error {
	// delete in batch and cache
	db.currentBatch.Delete(key)
	db.commitCache.Delete(key)
	return db.cache.Delete(key)
}

// DeleteSync implements DB.
func (db *DBCache) DeleteSync(key []byte) error {
	return db.Delete(key)
}

// Close implements DB.
func (db *DBCache) Close() error {
	db.commitCache.mtx.Lock()
	db.cache.mtx.Lock()
	defer db.commitCache.mtx.Unlock()
	defer db.cache.mtx.Unlock()

	for _, cBatch := range db.commitBatches {
		if cBatch != nil {
			cBatch.WriteSync()
		}
	}
	db.commitBatches = nil

	// add current batch to pending
	db.commitBatches = append(db.commitBatches, db.currentBatch)
	// reset
	db.commitCache.btree = db.cache.btree.Clone()
	db.currentBatch = newGoLevelDBBatch(db.db)
	db.cache.btree.Clear(true)
	return nil
}

// Print implements DB.
func (db *DBCache) Print() error {
	fmt.Println("Cache --")
	db.cache.Print()
	fmt.Println("DB --")
	db.db.Print()
	return nil
}

// Stats implements DB.
func (db *DBCache) Stats() map[string]string {

	stats := make(map[string]string)
	for k, v := range db.cache.Stats() {
		stats[k] = v
	}
	for k, v := range db.db.Stats() {
		stats[k] = v
	}
	return stats
}

// NewBatch implements DB.
func (db *DBCache) NewBatch() Batch {

	// new batch only in cache
	return newMemDBBatch(db.cache)
}

// Iterator implements DB.
// Takes out a read-lock on the database until the iterator is closed.
func (db *DBCache) Iterator(start, end []byte) (Iterator, error) {
	// iterator should be able to jump bewtween cache and db
	return newDBCacheIterator(db, start, end, false), nil
}

// ReverseIterator implements DB.
// Takes out a read-lock on the database until the iterator is closed.
func (db *DBCache) ReverseIterator(start, end []byte) (Iterator, error) {
	return newDBCacheIterator(db, start, end, true), nil
}

func (db *DBCache) printStats() {
	for k, v := range db.Stats() {
		fmt.Printf("%s: %s\n", k, v)
	}
}

func (db *DBCache) isPersistentKey(key []byte) bool {
	// currently we only have one persistent key
	return bytes.HasPrefix(key, db.persistentKey)
}
