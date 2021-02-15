package db

import (
	"fmt"
	"path/filepath"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewGoLevelDB(name, dir)
	}
	registerDBCreator(GoLevelDBBackend, dbCreator, false)
}

type GoLevelDB struct {
	db                      *leveldb.DB
	name                    string
	isCriticalZone          bool
	currentBatch            Batch
	waitingForCommitBatches []Batch
}

var _ DB = (*GoLevelDB)(nil)

func NewGoLevelDB(name string, dir string) (*GoLevelDB, error) {
	return NewGoLevelDBWithOpts(name, dir, nil)
}

func NewGoLevelDBWithOpts(name string, dir string, o *opt.Options) (*GoLevelDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	db, err := leveldb.OpenFile(dbPath, o)
	if err != nil {
		return nil, err
	}
	database := &GoLevelDB{
		db:   db,
		name: name,
	}
	fmt.Printf("Created database %s\n", name)
	return database, nil
}

func (db *GoLevelDB) SetCriticalZone() {
	fmt.Printf("Set critical zone for db: %s\n", db.name)
	db.currentBatch = newGoLevelDBBatch(db)
	db.isCriticalZone = true
}

func (db *GoLevelDB) ReleaseCriticalZone() error {
	fmt.Printf("Released critical zone (height-1) for db: %s (%d batches)\n", db.name, len(db.waitingForCommitBatches))
	for _, cBatch := range db.waitingForCommitBatches {
		if cBatch != nil {
			cBatch.WriteSync()
		}
	}
	db.waitingForCommitBatches = nil
	db.isCriticalZone = false
	db.waitingForCommitBatches = append(db.waitingForCommitBatches, db.currentBatch)
	db.currentBatch = nil
	return nil
}

// Get implements DB.
func (db *GoLevelDB) Get(key []byte) ([]byte, error) {
	key = nonNilBytes(key)
	// we do not need to get from gt here?
	res, err := db.db.Get(key, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return res, nil
}

// Has implements DB.
func (db *GoLevelDB) Has(key []byte) (bool, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

// Set implements DB.
func (db *GoLevelDB) Set(key []byte, value []byte) error {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	var err error = nil
	if db.isCriticalZone {
		db.currentBatch.Set(key, value)
	} else {
		// write directly to db
		err = db.db.Put(key, value, nil)
	}
	if err != nil {
		return err
	}
	return nil
}

func (db *GoLevelDB) ForceSet(key []byte, value []byte) error {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	err := db.db.Put(key, value, &opt.WriteOptions{Sync: true})
	if err != nil {
		return err
	}
	return nil
}

// SetSync implements DB.
func (db *GoLevelDB) SetSync(key []byte, value []byte) error {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	var err error = nil
	if db.isCriticalZone {
		db.currentBatch.Set(key, value)
	} else {
		// write directly to db
		err = db.db.Put(key, value, &opt.WriteOptions{Sync: true})
	}
	if err != nil {
		return err
	}
	return nil
}

// Delete implements DB.
func (db *GoLevelDB) Delete(key []byte) error {
	key = nonNilBytes(key)
	var err error = nil
	// TODO: if key is not found in batch, should we search in db?
	if db.isCriticalZone {
		db.currentBatch.Delete(key)
	} else {
		err = db.db.Delete(key, nil)
	}
	if err != nil {
		return err
	}
	return nil
}

// DeleteSync implements DB.
func (db *GoLevelDB) DeleteSync(key []byte) error {
	// TODO: should we find in batch first?
	key = nonNilBytes(key)
	err := db.db.Delete(key, &opt.WriteOptions{Sync: true})
	if err != nil {
		return err
	}
	return nil
}

func (db *GoLevelDB) DB() *leveldb.DB {
	return db.db
}

// Close implements DB.
func (db *GoLevelDB) Close() error {
	if err := db.db.Close(); err != nil {
		return err
	}
	return nil
}

// Print implements DB.
func (db *GoLevelDB) Print() error {
	str, err := db.db.GetProperty("leveldb.stats")
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", str)

	itr := db.db.NewIterator(nil, nil)
	for itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Stats implements DB.
func (db *GoLevelDB) Stats() map[string]string {
	keys := []string{
		"leveldb.num-files-at-level{n}",
		"leveldb.stats",
		"leveldb.sstables",
		"leveldb.blockpool",
		"leveldb.cachedblock",
		"leveldb.openedtables",
		"leveldb.alivesnaps",
		"leveldb.aliveiters",
	}

	stats := make(map[string]string)
	for _, key := range keys {
		str, err := db.db.GetProperty(key)
		if err == nil {
			stats[key] = str
		}
	}
	return stats
}

// NewBatch implements DB.
func (db *GoLevelDB) NewBatch() Batch {
	return newGoLevelDBBatch(db)
}

// Iterator implements DB.
func (db *GoLevelDB) Iterator(start, end []byte) (Iterator, error) {
	itr := db.db.NewIterator(nil, nil)
	return newGoLevelDBIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *GoLevelDB) ReverseIterator(start, end []byte) (Iterator, error) {
	itr := db.db.NewIterator(nil, nil)
	return newGoLevelDBIterator(itr, start, end, true), nil
}
