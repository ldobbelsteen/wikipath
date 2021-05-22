package main

import (
	"sync"
)

type SearchCache struct {
	data       map[Search][][]string
	mutex      sync.Mutex
	keys       []Search
	startIndex int
	endIndex   int
}

func NewSearchCache(size int) SearchCache {
	return SearchCache{
		data:       map[Search][][]string{},
		keys:       make([]Search, size),
		mutex:      sync.Mutex{},
		startIndex: 0,
		endIndex:   1,
	}
}

func (sc *SearchCache) Store(search Search, result [][]string) {
	sc.mutex.Lock()
	if _, alreadyExists := sc.data[search]; !alreadyExists {
		sc.data[search] = result
		sc.keys[sc.endIndex] = search
		sc.endIndex++
		if sc.endIndex >= len(sc.keys) {
			sc.endIndex = 0
		}
		if sc.endIndex == sc.startIndex {
			delete(sc.data, sc.keys[sc.startIndex])
			sc.startIndex++
			if sc.startIndex >= len(sc.keys) {
				sc.startIndex = 0
			}
		}
	}
	sc.mutex.Unlock()
}

func (sc *SearchCache) Find(search Search) [][]string {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	return sc.data[search]
}
