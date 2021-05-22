package main

import (
	"database/sql"
	"errors"
	"path/filepath"
	"regexp"
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
	pageToTitleQuery, err := conn.Prepare("SELECT title FROM pages WHERE id = ?")
	if err != nil {
		return nil, err
	}
	titleToPageQuery, err := conn.Prepare("SELECT id FROM pages WHERE title = ? LIMIT 1")
	if err != nil {
		return nil, err
	}
	randomTitleQuery, err := conn.Prepare("SELECT title FROM pages WHERE id = (abs(random()) % (SELECT (SELECT max(id) FROM pages) + 1))")
	if err != nil {
		return nil, err
	}
	followRedirQuery, err := conn.Prepare("SELECT redirect FROM pages WHERE id = ?")
	if err != nil {
		return nil, err
	}
	incomingQuery, err := conn.Prepare("SELECT source FROM links WHERE target = ?")
	if err != nil {
		return nil, err
	}
	outgoingQuery, err := conn.Prepare("SELECT target FROM links WHERE source = ?")
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

// Convert a page title to the corresponding ID
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
		db.randomTitleQuery.QueryRow().Scan(&title)
	}
	return title
}

// Get the incoming links of a page
func (db *Database) getIncoming(page int64) []int64 {
	result := []int64{}
	rows, err := db.incomingQuery.Query(page)
	if err != nil {
		return result
	}
	defer rows.Close()

	var id int64
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			continue
		}
		result = append(result, id)
	}

	return result
}

// Get the outgoing links of a page
func (db *Database) getOutgoing(page int64) []int64 {
	result := []int64{}
	rows, err := db.outgoingQuery.Query(page)
	if err != nil {
		return result
	}
	defer rows.Close()

	var id int64
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			continue
		}
		result = append(result, id)
	}

	return result
}

// Find all the paths of the shortest possible degree between two pages
func (db *Database) shortestPaths(search Search) [][]string {

	// Follow redirect if the source is a redirect
	var redirectedSource int64
	db.followRedirQuery.QueryRow(search.source).Scan(&redirectedSource)
	if redirectedSource != 0 {
		search.source = redirectedSource
	}

	// Follow redirect if the target is a redirect
	var redirectedTarget int64
	db.followRedirQuery.QueryRow(search.target).Scan(&redirectedTarget)
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
