package main

import (
	"database/sql"
	"errors"
	"log"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type Search struct {
	source       PageID
	target       PageID
	languageCode string
}

type Graph struct {
	PageNames     map[PageID]PageTitle `json:"pageNames"`
	OutgoingLinks map[PageID][]PageID  `json:"outgoingLinks"`
	PathCount     int                  `json:"pathCount"`
	PathDegree    int                  `json:"pathDegree"`
	SourcePage    PageID               `json:"sourcePage"`
	TargetPage    PageID               `json:"targetPage"`
	SourceIsRedir bool                 `json:"sourceIsRedir"`
	TargetIsRedir bool                 `json:"targetIsRedir"`
	LanguageCode  string               `json:"languageCode"`
}

type Database struct {
	connection       *sql.DB
	pageToTitleQuery *sql.Stmt
	randomPageQuery  *sql.Stmt
	followRedirQuery *sql.Stmt
	getIncomingQuery *sql.Stmt
	getOutgoingQuery *sql.Stmt
	DumpDate         string `json:"dumpDate"`
	LanguageName     string `json:"languageName"`
	LanguageCode     string `json:"languageCode"`
}

// Open a database for running queries on
func NewDatabase(path string, languages Languages) (*Database, error) {

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
	randomTitleQuery, err := conn.Prepare("SELECT page_id, title FROM titles WHERE page_id = (abs(random()) % (SELECT (SELECT max(page_id) FROM titles) + 1))")
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
		randomPageQuery:  randomTitleQuery,
		followRedirQuery: followRedirQuery,
		getIncomingQuery: incomingQuery,
		getOutgoingQuery: outgoingQuery,
		DumpDate:         info[2],
		LanguageName:     language.Name,
		LanguageCode:     language.Code,
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

// Pick a random page from all available pages
func (db *Database) RandomPage() Page {
	var id PageID
	var title PageTitle
	for id == 0 || title == "" {
		err := db.randomPageQuery.QueryRow().Scan(&id, &title)
		if err != nil && err != sql.ErrNoRows {
			log.Print("Error fetching random page: ", err)
		}
	}
	return Page{ID: id, Title: title}
}

// Get the incoming or outgoing links of a page
func (db *Database) GetLinks(page PageID, outgoing bool) []PageID {
	var delimited string
	var err error
	if outgoing {
		err = db.getOutgoingQuery.QueryRow(page).Scan(&delimited)
	} else {
		err = db.getIncomingQuery.QueryRow(page).Scan(&delimited)
	}
	if err != nil {
		if err != sql.ErrNoRows {
			log.Print("Error fetching links: ", err)
		}
		return []PageID{}
	}
	strings := strings.Split(delimited, "|")
	ids := make([]PageID, len(strings))
	for index, str := range strings {
		ids[index] = parsePageID(str)
	}
	return ids
}

// Find the paths of the shortest possible degree between two pages
func (db *Database) ShortestPaths(search Search) Graph {

	// Initialize graph
	graph := Graph{
		PageNames:     map[PageID]PageTitle{},
		OutgoingLinks: map[PageID][]PageID{},
		LanguageCode:  db.LanguageCode,
	}

	// Follow redirect if the source is a redirect
	var redirectedSource PageID
	err := db.followRedirQuery.QueryRow(search.source).Scan(&redirectedSource)
	if err != nil && err != sql.ErrNoRows {
		log.Print("Error following redirect: ", err)
	}
	if redirectedSource != 0 {
		search.source = redirectedSource
		graph.SourceIsRedir = true
	}
	graph.SourcePage = search.source

	// Follow redirect if the target is a redirect
	var redirectedTarget PageID
	err = db.followRedirQuery.QueryRow(search.target).Scan(&redirectedTarget)
	if err != nil && err != sql.ErrNoRows {
		log.Print("Error following redirect: ", err)
	}
	if redirectedTarget != 0 {
		search.target = redirectedTarget
		graph.TargetIsRedir = true
	}
	graph.TargetPage = search.target

	// Variables necessary for the search from both sides
	forwardParents := map[PageID][]PageID{search.source: {}}
	backwardParents := map[PageID][]PageID{search.target: {}}
	forwardQueue := []PageID{search.source}
	backwardQueue := []PageID{search.target}
	overlapping := map[PageID]bool{}
	forwardDepth := 0
	backwardDepth := 0

	// If the source is same as target, skip search
	if search.source == search.target {
		overlapping[search.source] = true
	}

	// Run bidirectional breadth-first search until the searches intersect
	for len(overlapping) == 0 && len(forwardQueue) > 0 && len(backwardQueue) > 0 {
		newQueue := []PageID{}
		newParents := map[PageID][]PageID{}
		if len(forwardQueue) < len(backwardQueue) {
			for _, page := range forwardQueue {
				for _, out := range db.GetLinks(page, true) {
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
				for _, in := range db.GetLinks(page, false) {
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

	// Recursively backtrack in either the forward or backward direction
	var backtrack func(PageID, map[PageID]int, bool) int
	backtrack = func(page PageID, counts map[PageID]int, forward bool) int {
		if _, occurred := graph.PageNames[page]; !occurred {
			title, err := db.PageToTitle(page)
			if err != nil {
				title = "Error"
			}
			graph.PageNames[page] = title
		}
		var parents []int64
		if forward {
			parents = backwardParents[page]
		} else {
			parents = forwardParents[page]
		}
		if len(parents) > 0 {
			duplicates := map[PageID]bool{}
			for _, parent := range parents {
				if !duplicates[parent] {
					duplicates[parent] = true
					if forward {
						graph.OutgoingLinks[page] = append(graph.OutgoingLinks[page], parent)
					} else {
						graph.OutgoingLinks[parent] = append(graph.OutgoingLinks[parent], page)
					}
					if count, isCounted := counts[parent]; isCounted {
						counts[page] += count
					} else {
						counts[page] += backtrack(parent, counts, forward)
					}
				}
			}
			return counts[page]
		} else {
			return 1
		}
	}

	// Backtrack from all overlapping nodes. Stores all page names and links
	// in the graph in the process. Also keeps track of the total number of paths.
	graph.PathDegree = forwardDepth + backwardDepth
	forwardPathCount := map[PageID]int{}
	backwardPathCount := map[PageID]int{}
	for overlap := range overlapping {
		forwardPathCount := backtrack(overlap, forwardPathCount, true)
		backwardPathCount := backtrack(overlap, backwardPathCount, false)
		graph.PathCount += forwardPathCount * backwardPathCount
	}

	// Sort all links to make the result deterministic
	for _, outgoing := range graph.OutgoingLinks {
		sort.Slice(outgoing, func(i, j int) bool { return outgoing[i] < outgoing[j] })
	}

	return graph
}
