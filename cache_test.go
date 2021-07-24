package main

import (
	"bytes"
	"math/rand"
	"testing"
)

func randomSearch() Search {
	return Search{
		source:       rand.Int63(),
		target:       rand.Int63(),
		languageCode: "en",
	}
}

func randomByteSlice(length int) []byte {
	slc := make([]byte, length)
	rand.Read(slc)
	return slc
}

func copyByteSlice(slc []byte) []byte {
	cpy := make([]byte, len(slc))
	copy(cpy, slc)
	return cpy
}

func checkInvariants(cache *SearchCache) bool {
	for index, search := range cache.keySlice {
		var inActiveRange bool
		if cache.keyEndIndex == cache.keyStartIndex {
			inActiveRange = false
		} else if cache.keyEndIndex > cache.keyStartIndex {
			inActiveRange = index >= cache.keyStartIndex && index < cache.keyEndIndex
		} else {
			inActiveRange = index >= cache.keyStartIndex || index < cache.keyEndIndex
		}

		// Check if the result that is supposed to be stored actually is
		if inActiveRange {
			if cache.Fetch(search) == nil {
				return false
			}
		}

		var storedCount int
		if cache.keyEndIndex == cache.keyStartIndex {
			storedCount = 0
		} else if cache.keyEndIndex > cache.keyStartIndex {
			storedCount = cache.keyEndIndex - cache.keyStartIndex
		} else {
			storedCount = cache.keyEndIndex + len(cache.keySlice) - cache.keyStartIndex
		}

		// Check if the number of stored searches corresponds with the number of results
		if storedCount != len(cache.resultData) {
			return false
		}

		// Check if the key slice has not grown exceedingly
		if len(cache.keySlice) > 3*storedCount {
			return false
		}

		var realSize int
		for _, result := range cache.resultData {
			realSize += len(result)
		}

		// Check if the cache size is accurate
		if realSize != cache.curByteSize {
			return false
		}

		// Check if the maximum size is not exceeded
		if cache.curByteSize > cache.maxByteSize {
			return false
		}

		// Check if indices are valid
		if cache.keyStartIndex >= len(cache.keySlice) {
			return false
		}
		if cache.keyEndIndex > len(cache.keySlice) {
			return false
		}
	}
	return true
}

func TestSearchCacheStandard(t *testing.T) {
	cache, _ := NewSearchCache(128)

	search1 := randomSearch()
	result1 := randomByteSlice(100)
	cache.Store(search1, result1)
	if !bytes.Equal(cache.Fetch(search1), result1) {
		t.Error("Expected search1 to be cached")
	}

	search2 := randomSearch()
	result2 := randomByteSlice(24)
	cache.Store(search2, result2)
	if !bytes.Equal(cache.Fetch(search1), result1) {
		t.Error("Expected search1 to be cached")
	}
	if !bytes.Equal(cache.Fetch(search2), result2) {
		t.Error("Expected search2 to be cached")
	}

	search3 := randomSearch()
	result3 := randomByteSlice(20)
	cache.Store(search3, result3)
	if bytes.Equal(cache.Fetch(search1), result1) {
		t.Error("Expected search1 to not be cached")
	}
	if !bytes.Equal(cache.Fetch(search2), result2) {
		t.Error("Expected search2 to be cached")
	}
	if !bytes.Equal(cache.Fetch(search3), result3) {
		t.Error("Expected search3 to be cached")
	}
}

func TestSearchCacheLarge(t *testing.T) {
	testCount := 128
	testSize := 131072
	searches := make([]Search, testCount)
	results := make([][]byte, testCount)
	for index := range searches {
		searches[index] = randomSearch()
		results[index] = randomByteSlice(testSize)
	}
	cache, err := NewSearchCache(testCount * testSize)
	if err != nil {
		t.Error("Unexpected error: ", err)
	}
	for index := range searches {
		cache.Store(searches[index], copyByteSlice(results[index]))
		if !checkInvariants(cache) {
			t.Fatal("Invariant violated")
		}
	}
	for index := range searches {
		if !bytes.Equal(cache.Fetch(searches[index]), results[index]) {
			t.Error("Expected search to be cached")
		}
	}
}

func TestSearchCacheHammer(t *testing.T) {
	cacheSize := 1048576
	maxResultSize := 8192
	storeCount := 2048
	cache, _ := NewSearchCache(cacheSize)

	for i := 0; i < storeCount; i++ {
		if !checkInvariants(cache) {
			t.Fatal("Invariant violated")
		}
		cache.Store(randomSearch(), randomByteSlice(rand.Intn(maxResultSize)))
	}
}

func TestSearchCacheEdge1(t *testing.T) {
	cache, err := NewSearchCache(0)
	if err != nil {
		t.Error("Unexpected error: ", err)
	}
	search := randomSearch()
	result := randomByteSlice(1)
	cache.Store(search, result)
	if bytes.Equal(cache.Fetch(search), result) {
		t.Error("Expected search to not be cached")
	}
}

func TestSearchCacheEdge2(t *testing.T) {
	if _, err := NewSearchCache(-1); err == nil {
		t.Error("Expected error on negative size")
	}
}

func TestSearchCacheEdge3(t *testing.T) {
	cache, err := NewSearchCache(128)
	if err != nil {
		t.Error("Unexpected error: ", err)
	}
	search := randomSearch()
	result := randomByteSlice(256)
	cache.Store(search, result)
	if bytes.Equal(cache.Fetch(search), result) {
		t.Error("Expected search to not be cached")
	}
}
