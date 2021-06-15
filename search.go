package main

import (
	"database/sql"
	"errors"
	"log"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

type Search struct {
	source PageID
	target PageID
}

type Database struct {
	connection       *sql.DB
	pageToTitleQuery *sql.Stmt
	titleToPageQuery *sql.Stmt
	randomTitleQuery *sql.Stmt
	followRedirQuery *sql.Stmt
	incomingQuery    *sql.Stmt
	outgoingQuery    *sql.Stmt
	DumpDate         string `json:"date"`
	LanguageName     string `json:"language"`
	LanguageCode     string `json:"code"`
	cacheMutex       sync.Mutex
	cacheSize        int
	cacheMax         int
	cacheIndex       int
	cacheKeys        []Search
	cacheData        map[Search][]byte
}

// Open a database for running queries on
func NewDatabase(path string, languages Languages, cacheSize int) (*Database, error) {

	// Open the database in read-only mode
	conn, err := sql.Open("sqlite3", "file:"+path+"?mode=ro")
	if err != nil {
		return nil, err
	}

	// Parse database info from the file's name
	filename := filepath.Base(path)
	info := regexp.MustCompile("(.*?)-(.*?)" + FILE_EXTENSION).FindStringSubmatch(filename)
	if len(info) != 3 {
		return nil, errors.New(filename + " file name has wrong format")
	}

	// Find database language based on the file's name
	language, err := languages.Search(info[1])
	if err != nil {
		return nil, err
	}

	// Prepare queries for performance reasons
	pageToTitleQuery, err := conn.Prepare("SELECT title FROM titles WHERE page_id = ?")
	if err != nil {
		return nil, err
	}
	titleToPageQuery, err := conn.Prepare("SELECT page_id FROM titles WHERE title = ?")
	if err != nil {
		return nil, err
	}
	randomTitleQuery, err := conn.Prepare("SELECT title FROM titles WHERE page_id = (abs(random()) % (SELECT (SELECT max(page_id) FROM titles) + 1))")
	if err != nil {
		return nil, err
	}
	followRedirQuery, err := conn.Prepare("SELECT target_id FROM redirects WHERE source_id = ?")
	if err != nil {
		return nil, err
	}
	incomingQuery, err := conn.Prepare("SELECT incoming_ids FROM incoming WHERE target_id = ?")
	if err != nil {
		return nil, err
	}
	outgoingQuery, err := conn.Prepare("SELECT outgoing_ids FROM outgoing WHERE source_id = ?")
	if err != nil {
		return nil, err
	}

	return &Database{
		connection:       conn,
		pageToTitleQuery: pageToTitleQuery,
		titleToPageQuery: titleToPageQuery,
		randomTitleQuery: randomTitleQuery,
		followRedirQuery: followRedirQuery,
		incomingQuery:    incomingQuery,
		outgoingQuery:    outgoingQuery,
		DumpDate:         info[2],
		LanguageName:     language.Name,
		LanguageCode:     language.Code,
		cacheMax:         cacheSize,
		cacheKeys:        []Search{},
		cacheData:        map[Search][]byte{},
	}, nil
}

// Convert a page ID to the corresponding title
func (db *Database) PageToTitle(page PageID) (PageTitle, error) {
	var title PageTitle
	err := db.pageToTitleQuery.QueryRow(page).Scan(&title)
	if err != nil {
		return "", err
	}
	return title, nil
}

// Convert a page title to the corresponding page ID
func (db *Database) TitleToPage(title PageTitle) (PageID, error) {
	var page PageID
	err := db.titleToPageQuery.QueryRow(title).Scan(&page)
	if err != nil {
		return 0, err
	}
	return page, nil
}

// Get a random page title
func (db *Database) RandomTitle() PageTitle {
	var title PageTitle
	for title == "" {
		err := db.randomTitleQuery.QueryRow().Scan(&title)
		if err != nil && err != sql.ErrNoRows {
			log.Print("Error fetching random title: ", err)
		}
	}
	return title
}

// Get the incoming links of a page
func (db *Database) GetIncoming(page PageID) []PageID {
	var incomingDelimited string
	err := db.incomingQuery.QueryRow(page).Scan(&incomingDelimited)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Print("Error fetching incoming links: ", err)
		}
		return []PageID{}
	}
	incomingStrings := strings.Split(incomingDelimited, "|")
	incoming := make([]PageID, len(incomingStrings))
	for index, str := range incomingStrings {
		incoming[index] = parsePageID(str)
	}
	return incoming
}

// Get the outgoing links of a page
func (db *Database) GetOutgoing(page PageID) []PageID {
	var outgoingDelimited PageTitle
	err := db.outgoingQuery.QueryRow(page).Scan(&outgoingDelimited)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Print("Error fetching outgoing links: ", err)
		}
		return []PageID{}
	}
	outgoingStrings := strings.Split(outgoingDelimited, "|")
	outgoing := make([]PageID, len(outgoingStrings))
	for index, str := range outgoingStrings {
		outgoing[index] = parsePageID(str)
	}
	return outgoing
}

// Find the paths of the shortest possible degree between two pages
func (db *Database) ShortestPaths(search Search) [][]PageID {

	// Follow redirect if the source is a redirect
	var redirectedSource PageID
	err := db.followRedirQuery.QueryRow(search.source).Scan(&redirectedSource)
	if err != nil && err != sql.ErrNoRows {
		log.Print("Error following redirect: ", err)
	}
	if redirectedSource != 0 {
		search.source = redirectedSource
	}

	// Follow redirect if the target is a redirect
	var redirectedTarget PageID
	err = db.followRedirQuery.QueryRow(search.target).Scan(&redirectedTarget)
	if err != nil && err != sql.ErrNoRows {
		log.Print("Error following redirect: ", err)
	}
	if redirectedTarget != 0 {
		search.target = redirectedTarget
	}

	// Variables necessary for the search
	forwardParents := map[PageID][]PageID{search.source: {}}
	backwardParents := map[PageID][]PageID{search.target: {}}
	forwardQueue := []PageID{search.source}
	backwardQueue := []PageID{search.target}
	overlapping := map[PageID]bool{}
	forwardDepth := 0
	backwardDepth := 0

	// Run bidirectional breadth-first search until the searches intersect
	for len(overlapping) == 0 && len(forwardQueue) > 0 && len(backwardQueue) > 0 {
		newQueue := []PageID{}
		newParents := map[PageID][]PageID{}
		if len(forwardQueue) < len(backwardQueue) {
			for _, page := range forwardQueue {
				for _, out := range db.GetOutgoing(page) {
					if _, visited := forwardParents[out]; !visited {
						newQueue = append(newQueue, out)
						newParents[out] = append(newParents[out], page)
						if _, visited := backwardParents[out]; visited {
							overlapping[out] = true
						}
					}
				}
			}
			forwardQueue = newQueue
			for child, parents := range newParents {
				forwardParents[child] = append(forwardParents[child], parents...)
			}
			forwardDepth++
		} else {
			for _, page := range backwardQueue {
				for _, in := range db.GetIncoming(page) {
					if _, visited := backwardParents[in]; !visited {
						newQueue = append(newQueue, in)
						newParents[in] = append(newParents[in], page)
						if _, visited := forwardParents[in]; visited {
							overlapping[in] = true
						}
					}
				}
			}
			backwardQueue = newQueue
			for child, parents := range newParents {
				backwardParents[child] = append(backwardParents[child], parents...)
			}
			backwardDepth++
		}
	}

	// Extract all of the possible paths from the search
	paths := [][]PageID{}
	pathLength := backwardDepth + forwardDepth + 1
	for overlap := range overlapping {
		forwardPaths := db.ExtractPaths(overlap, forwardParents)
		backwardPaths := db.ExtractPaths(overlap, backwardParents)
		for _, forwardPath := range forwardPaths {
			for _, backwardPath := range backwardPaths {
				fullPath := make([]PageID, pathLength)
				for index := 0; index < len(fullPath); index++ {
					if index < len(forwardPath) {
						reverseIndex := len(forwardPath) - index - 1
						fullPath[index] = forwardPath[reverseIndex]
					} else if index == len(forwardPath) {
						fullPath[index] = overlap
					} else {
						offsetIndex := index - len(forwardPath) - 1
						fullPath[index] = backwardPath[offsetIndex]
					}
				}
				paths = append(paths, fullPath)
			}
		}
	}

	return paths
}

// Backtrack from a page back to the source/target and return all of the possible paths
func (db *Database) ExtractPaths(page PageID, parents map[PageID][]PageID) [][]PageID {
	paths := [][]PageID{}
	var backtrack func(PageID, []PageID)
	backtrack = func(page PageID, path []PageID) {
		if allParents := parents[page]; len(allParents) == 0 {
			paths = append(paths, path)
		} else {
			occured := map[PageID]bool{}
			for _, parent := range allParents {
				if !occured[parent] {
					occured[parent] = true
					duplicate := make([]PageID, len(path), len(path)+1)
					copy(duplicate, path)
					duplicate = append(duplicate, parent)
					backtrack(parent, duplicate)
				}
			}
		}
	}
	backtrack(page, []PageID{})
	return paths
}

// Convert the page IDs in a path with their corresponding titles
func (db *Database) PathToTitles(path []PageID) []PageTitle {
	result := make([]PageTitle, len(path))
	for index, page := range path {
		title, err := db.PageToTitle(page)
		if err != nil {
			title = "Error"
		}
		result[index] = title
	}
	return result
}

// Convert the page IDs in a slice of paths with their corresponding titles
func (db *Database) PathsToTitles(paths [][]PageID) [][]PageTitle {
	result := make([][]PageTitle, len(paths))
	for index, path := range paths {
		result[index] = db.PathToTitles(path)
	}
	return result
}

// Store a search result into the database's internal cache
func (db *Database) CacheSet(search Search, result []byte) {
	db.cacheMutex.Lock()
	defer db.cacheMutex.Unlock()
	if _, alreadyExists := db.cacheData[search]; !alreadyExists {
		db.cacheData[search] = result
		db.cacheSize += len(result)

		if db.cacheIndex < len(db.cacheKeys) {
			db.cacheSize -= len(db.cacheData[db.cacheKeys[db.cacheIndex]])
			delete(db.cacheData, db.cacheKeys[db.cacheIndex])
			db.cacheKeys[db.cacheIndex] = search
		} else {
			db.cacheKeys = append(db.cacheKeys, search)
		}
		db.cacheIndex++

		if db.cacheSize > db.cacheMax && db.cacheIndex == len(db.cacheKeys) {
			db.cacheIndex = 0
		}
	}
}

// Get a search result from the database's internal cache
func (db *Database) CacheGet(search Search) []byte {
	db.cacheMutex.Lock()
	defer db.cacheMutex.Unlock()
	return db.cacheData[search]
}
