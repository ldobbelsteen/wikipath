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
	}
	for index := range searches {
		if !bytes.Equal(cache.Fetch(searches[index]), results[index]) {
			t.Error("Expected search to be cached")
		}
	}
}

func TestSearchCacheHammer(t *testing.T) {
	max := 12288
	size := 8388608
	count := 8096
	cache, _ := NewSearchCache(size)
	for i := 0; i < count; i++ {
		cache.Store(randomSearch(), randomByteSlice(rand.Intn(max)))
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
