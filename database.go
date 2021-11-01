package main

import (
	"database/sql"
	"errors"
	"sort"
)

type PageID = uint32

type Database struct {
	redirectQuery *sql.Stmt
	incomingQuery *sql.Stmt
	outgoingQuery *sql.Stmt
	BuildDate     string `json:"buildDate"`
	DumpDate      string `json:"dumpDate"`
	LangCode      string `json:"languageCode"`
	LangName      string `json:"languageName"`
	MaxPageID     PageID `json:"maxPageID"`
}

type Graph struct {
	Links       map[PageID][]PageID `json:"links"`
	Count       int                 `json:"count"`
	Degree      int                 `json:"degree"`
	Source      PageID              `json:"source"`
	Target      PageID              `json:"target"`
	SourceRedir bool                `json:"sourceRedir"`
	TargetRedir bool                `json:"targetRedir"`
}

func openDatabase(path string) (*Database, error) {
	database, err := sql.Open("sqlite3", "file:"+path+"?immutable=true")
	if err != nil {
		return nil, err
	}

	// Extract metadata from the database
	getMetadata := func(key string) (value string, err error) {
		row := database.QueryRow("SELECT value FROM metadata WHERE key = ?", key)
		err = row.Scan(&value)
		return
	}
	buildDate, err := getMetadata("buildDate")
	if err != nil {
		return nil, err
	}
	dumpDate, err := getMetadata("dumpDate")
	if err != nil {
		return nil, err
	}
	langCode, err := getMetadata("langCode")
	if err != nil {
		return nil, err
	}
	langName, err := getMetadata("langName")
	if err != nil {
		return nil, err
	}
	maxPageIdStr, err := getMetadata("maxPageID")
	if err != nil {
		return nil, err
	}
	maxPageID := parsePageID(maxPageIdStr)
	if maxPageID == 0 {
		return nil, errors.New("invalid maxPageID in metadata")
	}

	// Prepare queries for performance
	tx, err := database.Begin()
	if err != nil {
		return nil, err
	}
	redirectQuery, err := tx.Prepare("SELECT redirect FROM redirects WHERE id = ?")
	if err != nil {
		return nil, err
	}
	incomingQuery, err := tx.Prepare("SELECT incoming FROM incoming WHERE id = ?")
	if err != nil {
		return nil, err
	}
	outgoingQuery, err := tx.Prepare("SELECT outgoing FROM outgoing WHERE id = ?")
	if err != nil {
		return nil, err
	}

	return &Database{
		redirectQuery: redirectQuery,
		incomingQuery: incomingQuery,
		outgoingQuery: outgoingQuery,
		BuildDate:     buildDate,
		DumpDate:      dumpDate,
		LangCode:      langCode,
		LangName:      langName,
		MaxPageID:     maxPageID,
	}, nil
}

// Get the page to which a page redirects. Returns 0 if no redirect was found.
func (db *Database) getRedirect(page PageID) (PageID, error) {
	var result PageID
	err := db.redirectQuery.QueryRow(page).Scan(&result)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return result, nil
}

// Get the incoming or outgoing links of a page. Returns empty slice if no links were found.
func (db *Database) getLinks(page PageID, outgoing bool) ([]PageID, error) {
	var result []byte
	var err error

	// Query the database based on direction
	if outgoing {
		err = db.outgoingQuery.QueryRow(page).Scan(&result)
	} else {
		err = db.incomingQuery.QueryRow(page).Scan(&result)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return []PageID{}, nil
		}
		return nil, err
	}

	// Convert the blob back to a slice of pages
	return bytesToPages(result)
}

// Find the paths of the shortest possible degree between two pages
func (db *Database) shortestPaths(source PageID, target PageID, languageCode string) (*Graph, error) {
	graph := &Graph{}

	// Follow any redirects for the source and target
	if sourceRedir, err := db.getRedirect(source); err != nil {
		return nil, err
	} else if sourceRedir != 0 {
		source = sourceRedir
		graph.SourceRedir = true
	}
	graph.Source = source
	if targetRedir, err := db.getRedirect(target); err != nil {
		return nil, err
	} else if targetRedir != 0 {
		target = targetRedir
		graph.TargetRedir = true
	}
	graph.Target = target

	// Variables necessary for the search from both sides
	forwardParents := map[PageID][]PageID{source: {}}
	backwardParents := map[PageID][]PageID{target: {}}
	forwardQueue := []PageID{source}
	backwardQueue := []PageID{target}
	overlapping := map[PageID]bool{}
	forwardDepth := 0
	backwardDepth := 0

	// If the source is same as target, skip search
	if source == target {
		overlapping[source] = true
	}

	var member struct{}

	// Run bidirectional breadth-first search until the searches intersect
	for len(overlapping) == 0 && len(forwardQueue) > 0 && len(backwardQueue) > 0 {
		newParents := map[PageID]map[PageID]struct{}{}
		forwardLength := len(forwardQueue)
		backwardLength := len(backwardQueue)
		if forwardLength < backwardLength {
			for i := 0; i < forwardLength; i++ {
				page := forwardQueue[0]
				forwardQueue = forwardQueue[1:]
				outgoing, err := db.getLinks(page, true)
				if err != nil {
					return nil, err
				}
				for _, out := range outgoing {
					if _, visited := forwardParents[out]; !visited {
						forwardQueue = append(forwardQueue, out)
						if set, exists := newParents[out]; exists {
							set[page] = member
						} else {
							newParents[out] = map[PageID]struct{}{page: member}
						}
						if _, visited := backwardParents[out]; visited {
							overlapping[out] = true
						}
					}
				}
			}
			for child, parents := range newParents {
				for parent := range parents {
					forwardParents[child] = append(forwardParents[child], parent)
				}
			}
			forwardDepth++
		} else {
			for i := 0; i < backwardLength; i++ {
				page := backwardQueue[0]
				backwardQueue = backwardQueue[1:]
				incoming, err := db.getLinks(page, false)
				if err != nil {
					return nil, err
				}
				for _, in := range incoming {
					if _, visited := backwardParents[in]; !visited {
						backwardQueue = append(backwardQueue, in)
						if set, exists := newParents[in]; exists {
							set[page] = member
						} else {
							newParents[in] = map[PageID]struct{}{page: member}
						}
						if _, visited := forwardParents[in]; visited {
							overlapping[in] = true
						}
					}
				}
			}
			for child, parents := range newParents {
				for parent := range parents {
					backwardParents[child] = append(backwardParents[child], parent)
				}
			}
			backwardDepth++
		}
	}

	// Backtrack from all overlapping pages. Stores the total number of paths
	// and all links in the final paths into the graph.
	graph.Links = map[PageID][]PageID{}
	forwardPathCounts := map[PageID]int{}
	backwardPathCounts := map[PageID]int{}
	for overlap := range overlapping {
		forwardPathCount := extractPathCount(overlap, forwardPathCounts, true, backwardParents, graph.Links)
		backwardPathCount := extractPathCount(overlap, backwardPathCounts, false, forwardParents, graph.Links)
		graph.Count += forwardPathCount * backwardPathCount
	}
	if graph.Count == 0 {
		graph.Degree = 0
	} else {
		graph.Degree = forwardDepth + backwardDepth
	}

	// Sort all links to make the result deterministic
	for _, outgoing := range graph.Links {
		sort.Slice(outgoing, func(i, j int) bool { return outgoing[i] < outgoing[j] })
	}

	return graph, nil
}

// Extract the number of possible paths from a page to the source or target.
// Uses path count memoization to reduce recursions. Stores any occurred links into the links map.
func extractPathCount(page PageID, counts map[PageID]int, forward bool, parents map[PageID][]PageID, links map[PageID][]PageID) int {
	directParents := parents[page]
	if len(directParents) > 0 {
		duplicates := map[PageID]bool{}
		for _, parent := range directParents {
			if !duplicates[parent] {
				duplicates[parent] = true
				if forward {
					links[page] = append(links[page], parent)
				} else {
					links[parent] = append(links[parent], page)
				}
				if count, isCounted := counts[parent]; isCounted {
					counts[page] += count
				} else {
					counts[page] += extractPathCount(parent, counts, forward, parents, links)
				}
			}
		}
		return counts[page]
	}
	return 1
}
