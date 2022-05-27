package main

import (
	"context"
	"database/sql"
	"errors"
	"sort"
)

type PageId = uint32

type Database struct {
	self              *sql.DB
	redirectStatement *sql.Stmt
	incomingStatement *sql.Stmt
	outgoingStatement *sql.Stmt
	BuildDate         string `json:"buildDate"`
	DumpDate          string `json:"dumpDate"`
	LanguageCode      string `json:"languageCode"`
	LanguageName      string `json:"languageName"`
	LargestPageId     PageId `json:"largestPageId"`
}

type Transaction struct {
	self     *sql.Tx
	redirect *sql.Stmt
	incoming *sql.Stmt
	outgoing *sql.Stmt
	context  context.Context
}

type Graph struct {
	LanguageCode  string              `json:"languageCode"`
	Links         map[PageId][]PageId `json:"links"`
	PathCount     int                 `json:"pathCount"`
	PathDegrees   int                 `json:"pathDegrees"`
	SourceId      PageId              `json:"sourceId"`
	TargetId      PageId              `json:"targetId"`
	SourceIsRedir bool                `json:"sourceIsRedir"`
	TargetIsRedir bool                `json:"targetIsRedir"`
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
	largestPageIdStr, err := getMetadata("largestPageId")
	if err != nil {
		return nil, err
	}
	largestPageId := parsePageId(largestPageIdStr)
	if largestPageId == 0 {
		return nil, errors.New("invalid largestPageId in metadata")
	}

	// Prepare statement templates for later transactions
	redirectTemplate, err := database.Prepare("SELECT redirect FROM redirects WHERE id = ?")
	if err != nil {
		return nil, err
	}
	incomingTemplate, err := database.Prepare("SELECT incoming FROM incoming WHERE id = ?")
	if err != nil {
		return nil, err
	}
	outgoingTemplate, err := database.Prepare("SELECT outgoing FROM outgoing WHERE id = ?")
	if err != nil {
		return nil, err
	}

	return &Database{
		self:              database,
		redirectStatement: redirectTemplate,
		incomingStatement: incomingTemplate,
		outgoingStatement: outgoingTemplate,
		BuildDate:         buildDate,
		DumpDate:          dumpDate,
		LanguageCode:      langCode,
		LanguageName:      langName,
		LargestPageId:     largestPageId,
	}, nil
}

// Run a function that takes a single transaction in a certain context
func (db *Database) runTransaction(ctx context.Context, fn func(tx Transaction)) error {
	tx, err := db.self.Begin()
	if err != nil {
		return err
	}
	fn(Transaction{
		self:     tx,
		redirect: tx.Stmt(db.redirectStatement),
		incoming: tx.Stmt(db.incomingStatement),
		outgoing: tx.Stmt(db.outgoingStatement),
		context:  ctx,
	})
	return tx.Commit()
}

// Get the page to which a page redirects. Returns 0 if no redirect was found.
func (tx Transaction) getRedirect(page PageId) (PageId, error) {
	var result PageId
	err := tx.redirect.QueryRowContext(tx.context, page).Scan(&result)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return result, nil
}

// Get the incoming links of a page. Returns empty slice if no links were found.
func (tx Transaction) getIncoming(page PageId) ([]PageId, error) {
	var data []byte
	if err := tx.incoming.QueryRowContext(tx.context, page).Scan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []PageId{}, nil
		} else {
			return nil, err
		}
	}
	return bytesToPages(data)
}

// Get the outgoing links of a page. Returns empty slice if no links were found.
func (tx Transaction) getOutgoing(page PageId) ([]PageId, error) {
	var data []byte
	if err := tx.outgoing.QueryRowContext(tx.context, page).Scan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []PageId{}, nil
		} else {
			return nil, err
		}
	}
	return bytesToPages(data)
}

// Find the paths of the shortest possible degree between a source and target page
func (tx Transaction) getShortestPaths(langCode string, source PageId, target PageId) (*Graph, error) {
	graph := &Graph{
		Links:         map[PageId][]PageId{},
		PathCount:     0,
		PathDegrees:   0,
		SourceId:      source,
		TargetId:      target,
		SourceIsRedir: false,
		TargetIsRedir: false,
		LanguageCode:  langCode,
	}

	// Follow any redirects for the source and target
	if sourceRedir, err := tx.getRedirect(source); err != nil {
		return nil, err
	} else if sourceRedir != 0 {
		source = sourceRedir
		graph.SourceIsRedir = true
		graph.SourceId = source
	}
	if targetRedir, err := tx.getRedirect(target); err != nil {
		return nil, err
	} else if targetRedir != 0 {
		target = targetRedir
		graph.TargetIsRedir = true
		graph.TargetId = target
	}

	forwardParents := map[PageId][]PageId{source: {}}
	backwardParents := map[PageId][]PageId{target: {}}
	forwardQueue := []PageId{source}
	backwardQueue := []PageId{target}
	overlapping := map[PageId]bool{}
	forwardDepth := 0
	backwardDepth := 0

	// If the source is same as target, skip search
	if source == target {
		overlapping[source] = true
	}

	var member struct{}

	// Run bidirectional breadth-first search until the searches intersect
	for len(overlapping) == 0 && len(forwardQueue) > 0 && len(backwardQueue) > 0 {
		newParents := map[PageId]map[PageId]struct{}{}
		forwardLength := len(forwardQueue)
		backwardLength := len(backwardQueue)
		if forwardLength < backwardLength {
			for i := 0; i < forwardLength; i++ {
				page := forwardQueue[0]
				forwardQueue = forwardQueue[1:]
				outgoing, err := tx.getOutgoing(page)
				if err != nil {
					return nil, err
				}
				for _, out := range outgoing {
					if _, visited := forwardParents[out]; !visited {
						forwardQueue = append(forwardQueue, out)
						if set, exists := newParents[out]; exists {
							set[page] = member
						} else {
							newParents[out] = map[PageId]struct{}{page: member}
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
				incoming, err := tx.getIncoming(page)
				if err != nil {
					return nil, err
				}
				for _, in := range incoming {
					if _, visited := backwardParents[in]; !visited {
						backwardQueue = append(backwardQueue, in)
						if set, exists := newParents[in]; exists {
							set[page] = member
						} else {
							newParents[in] = map[PageId]struct{}{page: member}
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
	forwardPathCounts := map[PageId]int{}
	backwardPathCounts := map[PageId]int{}
	for overlap := range overlapping {
		forwardPathCount := extractPathCount(overlap, forwardPathCounts, true, backwardParents, graph.Links)
		backwardPathCount := extractPathCount(overlap, backwardPathCounts, false, forwardParents, graph.Links)
		graph.PathCount += forwardPathCount * backwardPathCount
	}
	if graph.PathCount != 0 {
		graph.PathDegrees = forwardDepth + backwardDepth
	}

	// Sort all links to make the result deterministic
	for _, outgoing := range graph.Links {
		sort.Slice(outgoing, func(i, j int) bool { return outgoing[i] < outgoing[j] })
	}

	return graph, nil
}

// Extract the number of possible paths from a page to the source or target.
// Uses path count memoization to reduce recursions. Stores any occurred links into the links map.
func extractPathCount(page PageId, counts map[PageId]int, forward bool, parents map[PageId][]PageId, links map[PageId][]PageId) int {
	directParents := parents[page]
	if len(directParents) > 0 {
		duplicates := map[PageId]bool{}
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
