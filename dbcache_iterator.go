package db

type dBCacheIterator struct {
	dbItr          *goLevelDBIterator
	cacheItr       *memDBIterator
	commitCacheItr *memDBIterator
	start          []byte
	end            []byte
	isReverse      bool
	isInvalid      bool
}

var _ Iterator = (*dBCacheIterator)(nil)

func newDBCacheIterator(db *DBCache, start, end []byte, isReverse bool) *dBCacheIterator {
	itr := db.db.db.NewIterator(nil, nil)
	// when allocating the iterators, they will seek for the start and end
	// if the keys are not found, they should default to min and max
	dbItr := newGoLevelDBIterator(itr, start, end, isReverse)
	cacheItr := newMemDBIterator(db.cache, start, end, isReverse)
	commitCacheItr := newMemDBIterator(db.commitCache, start, end, isReverse)

	return &dBCacheIterator{
		dbItr:          dbItr,
		cacheItr:       cacheItr,
		commitCacheItr: commitCacheItr,
		start:          start,
		end:            end,
		isReverse:      isReverse,
		isInvalid:      false,
	}
}

// Domain implements Iterator.
func (itr *dBCacheIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Valid implements Iterator.
func (itr *dBCacheIterator) Valid() bool {

	if !itr.cacheItr.Valid() && !itr.dbItr.Valid() && !itr.commitCacheItr.Valid() {
		return false
	}
	return true
}

// Key implements Iterator.
func (itr *dBCacheIterator) Key() []byte {

	if !itr.isReverse {
		// check cache first
		switch {
		case itr.cacheItr.Valid():
			return itr.cacheItr.Key()
		case itr.commitCacheItr.Valid():
			return itr.commitCacheItr.Key()
		case itr.dbItr.Valid():
			return itr.dbItr.Key()
		}
	}
	// check db first
	switch {
	case itr.dbItr.Valid():
		return itr.dbItr.Key()
	case itr.commitCacheItr.Valid():
		return itr.commitCacheItr.Key()
	case itr.cacheItr.Valid():
		return itr.cacheItr.Key()
	}
	return nil
}

// Value implements Iterator.
func (itr *dBCacheIterator) Value() []byte {

	if !itr.isReverse {
		// check cache first
		switch {
		case itr.cacheItr.Valid():
			return itr.cacheItr.Value()
		case itr.commitCacheItr.Valid():
			return itr.commitCacheItr.Value()
		case itr.dbItr.Valid():
			return itr.dbItr.Value()
		}
	}
	// check db first
	switch {
	case itr.dbItr.Valid():
		return itr.dbItr.Value()
	case itr.commitCacheItr.Valid():
		return itr.commitCacheItr.Value()
	case itr.cacheItr.Valid():
		return itr.cacheItr.Value()
	}
	return nil
}

// Next implements Iterator.
func (itr *dBCacheIterator) Next() {

	if !itr.isReverse {
		// check cache first
		switch {
		case itr.cacheItr.Valid():
			itr.cacheItr.Next()
		case itr.commitCacheItr.Valid():
			itr.commitCacheItr.Next()
		case itr.dbItr.Valid():
			itr.dbItr.Next()
		}
	} else {
		// check db first
		switch {
		case itr.dbItr.Valid():
			itr.dbItr.Next()
		case itr.commitCacheItr.Valid():
			itr.commitCacheItr.Next()
		case itr.cacheItr.Valid():
			itr.cacheItr.Next()
		}
	}
}

// Error implements Iterator.
func (itr *dBCacheIterator) Error() error {
	var err error
	err = itr.cacheItr.Error()
	if err != nil {
		return err
	}
	err = itr.commitCacheItr.Error()
	if err != nil {
		return err
	}
	err = itr.dbItr.Error()
	if err != nil {
		return err
	}
	// return itr.source.Error()
	return nil
}

// Close implements Iterator.
func (itr *dBCacheIterator) Close() {
	itr.cacheItr.Close()
	itr.commitCacheItr.Close()
	itr.dbItr.Close()
}

func (itr *dBCacheIterator) assertNoError() {
	err := itr.Error()
	if err != nil {
		panic(err)
	}
}

func (itr dBCacheIterator) assertIsValid() {
	if !itr.Valid() {
		panic("dBCacheIterator is invalid")
	}
}
