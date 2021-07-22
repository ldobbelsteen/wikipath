package main

import (
	"errors"
	"sync"
)

type SearchCache struct {
	mutex         sync.Mutex
	curByteSize   int
	maxByteSize   int
	keyStartIndex int
	keyEndIndex   int
	keySlice      []Search
	resultData    map[Search][]byte
}

// Cache for storing shortest path search results
func NewSearchCache(size int) (*SearchCache, error) {
	if size < 0 {
		return nil, errors.New("invalid search cache size")
	}
	return &SearchCache{
		maxByteSize: size,
		keySlice:    []Search{},
		resultData:  map[Search][]byte{},
	}, nil
}

// Fetch a search result from the cache
func (db *SearchCache) Fetch(s Search) []byte {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.resultData[s]
}

// Store a search result into the cache
func (db *SearchCache) Store(s Search, res []byte) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// Remove the oldest inserted result from the cache
	purgeOldest := func() {
		db.curByteSize -= len(db.resultData[db.keySlice[db.keyStartIndex]])
		delete(db.resultData, db.keySlice[db.keyStartIndex])
		db.keyStartIndex++
		if db.keyStartIndex == len(db.keySlice) {
			db.keyStartIndex = 0
		}
	}

	// Ignore the result if it has already been stored
	if _, alreadyStored := db.resultData[s]; !alreadyStored {
		db.resultData[s] = res
		db.curByteSize += len(res)
		if db.keyEndIndex < len(db.keySlice) {
			db.keySlice[db.keyEndIndex] = s
		} else {
			db.keySlice = append(db.keySlice, s)
		}
		db.keyEndIndex++
		if db.keyEndIndex == db.keyStartIndex {
			purgeOldest()
		}

		// If the size is exceeded, purge cached results starting from
		// the oldest inserted result until the size is below the threshold
		if db.curByteSize > db.maxByteSize {
			for db.curByteSize > db.maxByteSize {
				purgeOldest()
			}
			if db.keyEndIndex == len(db.keySlice) && db.keyStartIndex*2 > db.keyEndIndex {
				db.keyEndIndex = 0
			}
		}
	}
}
