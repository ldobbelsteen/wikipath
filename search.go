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
	language string
	source   int64
	target   int64
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
}

// Open a database for running queries on
func NewDatabase(path string, finder LanguageFinder) (*Database, error) {

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
	language, err := finder.Search(info[1])
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
	}, nil
}

// Convert a page ID to the corresponding title
func (db *Database) pageToTitle(page int64) (string, error) {
	var title string
	err := db.pageToTitleQuery.QueryRow(page).Scan(&title)
	if err != nil {
		return "", err
	}
	return title, nil
}

// Convert a page title to the corresponding page ID
func (db *Database) titleToPage(title string) (int64, error) {
	var page int64
	err := db.titleToPageQuery.QueryRow(title).Scan(&page)
	if err != nil {
		return 0, err
	}
	return page, nil
}

// Get a random page title
func (db *Database) randomTitle() string {
	var title string
	for title == "" {
		err := db.randomTitleQuery.QueryRow().Scan(&title)
		if err != nil && err != sql.ErrNoRows {
			log.Print("Error fetching random title: ", err)
		}
	}
	return title
}

// Get the incoming links of a page
func (db *Database) getIncoming(page int64) []int64 {
	var incomingDelimited string
	err := db.incomingQuery.QueryRow(page).Scan(&incomingDelimited)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Print("Error fetching incoming links: ", err)
		}
		return []int64{}
	}
	incomingStrings := strings.Split(incomingDelimited, "|")
	incoming := make([]int64, len(incomingStrings))
	for index, str := range incomingStrings {
		incoming[index] = parsePageID(str)
	}
	return incoming
}

// Get the outgoing links of a page
func (db *Database) getOutgoing(page int64) []int64 {
	var outgoingDelimited string
	err := db.outgoingQuery.QueryRow(page).Scan(&outgoingDelimited)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Print("Error fetching outgoing links: ", err)
		}
		return []int64{}
	}
	outgoingStrings := strings.Split(outgoingDelimited, "|")
	outgoing := make([]int64, len(outgoingStrings))
	for index, str := range outgoingStrings {
		outgoing[index] = parsePageID(str)
	}
	return outgoing
}

// Find all the paths of the shortest possible degree between two pages
func (db *Database) shortestPaths(search Search) [][]string {

	// Follow redirect if the source is a redirect
	var redirectedSource int64
	err := db.followRedirQuery.QueryRow(search.source).Scan(&redirectedSource)
	if err != nil && err != sql.ErrNoRows {
		log.Print("Error following redirect: ", err)
	}
	if redirectedSource != 0 {
		search.source = redirectedSource
	}

	// Follow redirect if the target is a redirect
	var redirectedTarget int64
	err = db.followRedirQuery.QueryRow(search.target).Scan(&redirectedTarget)
	if err != nil && err != sql.ErrNoRows {
		log.Print("Error following redirect: ", err)
	}
	if redirectedTarget != 0 {
		search.target = redirectedTarget
	}

	// Maps pages to their parents and/or children if known
	parents := map[int64]int64{search.source: search.source}
	children := map[int64]int64{search.target: search.target}

	// The current queues of the forward and backward BFSes
	forwardQueue := []int64{search.source}
	backwardQueue := []int64{search.target}
	forwardDepth := 0
	backwardDepth := 0

	// Slice of intersecting pages between the forward and backward searches
	intersecting := []int64{}
	if search.source == search.target {
		intersecting = append(intersecting, search.source)
	}

	// Run bidirectional breadth-first search on the database
	for len(intersecting) == 0 && len(forwardQueue) > 0 && len(backwardQueue) > 0 {
		if len(backwardQueue) > len(forwardQueue) {
			forwardDepth++
			newQueue := []int64{}
			for _, page := range forwardQueue {
				outgoing := db.getOutgoing(page)
				for _, out := range outgoing {
					if _, exists := parents[out]; !exists {
						parents[out] = page
						newQueue = append(newQueue, out)
						if _, exists := children[out]; exists {
							intersecting = append(intersecting, out)
						}
					}
				}
			}
			forwardQueue = newQueue
		} else {
			backwardDepth++
			newQueue := []int64{}
			for _, page := range backwardQueue {
				incoming := db.getIncoming(page)
				for _, in := range incoming {
					if _, exists := children[in]; !exists {
						children[in] = page
						newQueue = append(newQueue, in)
						if _, exists := parents[in]; exists {
							intersecting = append(intersecting, in)
						}
					}
				}
			}
			backwardQueue = newQueue
		}
	}

	// If any intersection and thus path was found, track back and forward to find it
	if len(intersecting) > 0 {
		paths := make([][]string, 0, len(intersecting))

		for _, intersect := range intersecting {
			path := make([]string, forwardDepth+backwardDepth+1)

			// Go up the chain and get titles
			for i, t := forwardDepth, intersect; i >= 0; i-- {
				title, err := db.pageToTitle(t)
				if err != nil {
					title = "Error"
				}
				path[i] = title
				t = parents[t]
			}

			// Go down hte chain and get titles
			for i, t := forwardDepth, intersect; i < len(path); i++ {
				title, err := db.pageToTitle(t)
				if err != nil {
					title = "Error"
				}
				path[i] = title
				t = children[t]
			}

			paths = append(paths, path)
		}

		return paths
	}

	// Return empty path if none was found
	return [][]string{}
}

type SearchCache struct {
	data  map[*Search][][]string
	keys  []*Search
	start int
	end   int
	mutex sync.Mutex
}

func NewSearchCache(size int) *SearchCache {
	return &SearchCache{
		data: make(map[*Search][][]string, size),
		keys: make([]*Search, size),
	}
}

func (c *SearchCache) Store(search *Search, result [][]string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, alreadyExists := c.data[search]; !alreadyExists {
		c.data[search] = result
		c.keys[c.end] = search
		c.end++
		if c.end >= len(c.keys) {
			c.end = 0
		}
		if c.end == c.start {
			delete(c.data, c.keys[c.start])
			c.start++
			if c.start >= len(c.keys) {
				c.start = 0
			}
		}
	}
}

func (sc *SearchCache) Find(search *Search) [][]string {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	return sc.data[search]
}
