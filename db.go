package benzene

// the DB is benzene database
type Db struct {
	// MemDb
	memDb MemDB
}
// Open opens or creates a DB for the given storage.
// The DB will be created if not exist, unless ErrorIfMissing is true.
// Also, if ErrorIfExist is true and the DB exist Open will returns
// os.ErrExist error.
//
// Open will return an error with type of ErrCorrupted if corruption
// detected in the DB. Use errors.IsCorrupted to test whether an error is
// due to corruption. Corrupted DB can be recovered with Recover function.
//
// The returned DB instance is safe for concurrent use.
// The DB must be closed after use, by calling Close method.
func Open() (db *Db, err error) {
	//1.``
	return
}

// Close closes the DB. This will also releases any outstanding snapshot,
// abort any in-flight compaction and discard open transaction.
//
// It is not safe to close a DB until all outstanding iterators are released.
// It is valid to call Close multiple times. Other methods should not be
// called after the DB has been closed.
func Close(db *Db) () {

}

func (db *Db) Get() (err error) {
	return
}

func (db *Db) Insert() (err error) {
	return
}


