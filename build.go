package main

import (
	"database/sql"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/cheggaaa/pb/v3"
)

const BUFFER_SIZE = 24576

// Build a new database from scratch using a set of dump files and the desired path of the database
// This process is language agnostic as it is determined by the dump files that are supplied
func build(path string, files LocalDumpFiles) error {

	// Delete any previous database
	err := os.Remove(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	// Create new database and open it with full privileges
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=rwc&_journal=MEMORY")
	if err != nil {
		return err
	}
	defer db.Close()

	// Start a transaction to improve insert performance
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Commit()

	// Create the main pages table. It represents all the pages by its ID, title and the page it redirects to.
	// It also contains the IDs of the pages that link to it (incoming) and the pages it links to (outgoing),
	// in the form of a string with the IDs delimited by commas. The redirect column is NULL if the page is not a
	// redirect and the ID of the target page if it is.
	_, err = tx.Exec(`
		CREATE TABLE pages (
			id INTEGER PRIMARY KEY,
			title TEXT NOT NULL,
			redirect INTEGER,
			incoming TEXT,
			outgoing TEXT
		);
	`)
	if err != nil {
		return err
	}

	// Parse the page dump file and ingest the resulting pages into the database with redirect 0 for now; the redirects will be inserted later.
	// A map is also created, mapping a page title to its page ID which is useful for the redirects and links parsing later on.
	log.Print("Parsing & ingesting page dump file...")
	pageChan := make(chan Page, BUFFER_SIZE)
	go pageDumpParse(files.pageFilePath, pageChan)
	titler := map[string]int64{}
	insertPage, err := tx.Prepare("INSERT OR REPLACE INTO pages VALUES (?, ?, NULL, NULL, NULL)")
	if err != nil {
		return err
	}
	for page := range pageChan {
		titler[page.title] = page.id
		_, err := insertPage.Exec(page.id, page.title)
		if err != nil {
			return err
		}
	}

	// Create index to optimize getting a page ID by a page's title
	log.Print("Creating title finding optimization index...")
	_, err = tx.Exec("CREATE INDEX titler ON pages (title)")
	if err != nil {
		return err
	}

	// Parse the redirects dump file and create a map that maps a page ID to the page ID it redirects to
	log.Print("Parsing redirect dump file...")
	redirectChan := make(chan Redir, BUFFER_SIZE)
	go redirDumpParse(files.redirFilePath, titler, redirectChan)
	redirects := map[int64]int64{}
	for redirect := range redirectChan {
		redirects[redirect.source] = redirect.target
	}

	// Loop over the redirects map and update the targets of redirects that have another redirect as a target. This also makes sure to break
	// any cyclic redirects, favoring the deepest chain of redirects before a cycle occurs. Cyclic redirects should only occur when dumps are
	// created in the middle of page edits where titles are changed causing redirects to be messed up, which is very rare. All targets in the
	// map are now guaranteed to not be redirects themselves. The redirects are also inserted into the database.
	log.Print("Cleaning up & ingesting redirects...")
	updateRedirect, err := tx.Prepare("UPDATE pages SET redirect = ? WHERE id = ?")
	if err != nil {
		return err
	}
	bar := pb.StartNew(len(redirects))
	for source, target := range redirects {
		bar.Increment()
		_, targetIsRedir := redirects[target]
		if targetIsRedir {
			encountered := []int64{target} // Keep track of followed redirects
			for {
				tempTarget, isRedirect := redirects[encountered[len(encountered)-1]]
				if !isRedirect {
					break // Exit when the target is not a redirect anymore
				}
				for _, enc := range encountered {
					if tempTarget == enc { // Break cyclic redirect chains
						delete(redirects, encountered[len(encountered)-1])
					}
				}
				encountered = append(encountered, tempTarget)
			}
			target = encountered[len(encountered)-1]
			redirects[source] = target
		}
		_, err := updateRedirect.Exec(target, source)
		if err != nil {
			return err
		}
	}
	bar.Finish()

	// Parse the pagelinks dump file. It keeps a map of incoming and outgoing links for each page in memory,
	// to prevent having to do an insert for each link. This however means a lot of memory will be used,
	// which is why in parallel the memory usage is checked every second. If the maximum memory usage is
	// exceeded, the parsing process is temporarily paused and the maps are (partially) flushed to the
	// database. At the end all of the remaining in-memory links are also flushed to the database.
	log.Print("Parsing link dump file...")
	linkChan := make(chan Link, BUFFER_SIZE)
	go linkDumpParse(files.linkFilePath, titler, redirects, linkChan)
	// memoryLimitReached := false
	// go func() {
	// 	var stats runtime.MemStats
	// 	for {
	// 		runtime.ReadMemStats(&stats)
	// 		memoryLimitReached = stats.Alloc > maxMemory
	// 		time.Sleep(time.Second)
	// 	}
	// }()
	insertIncomingQuery, err := tx.Prepare("UPDATE pages SET incoming = CASE WHEN incoming IS NULL THEN '' ELSE incoming || ',' END || ? WHERE id = ?")
	if err != nil {
		return err
	}
	insertIncoming := func(target int64, sources []int64) error {
		stringSources := make([]string, len(sources))
		for i, v := range sources {
			stringSources[i] = strconv.FormatInt(v, 10)
		}
		_, err := insertIncomingQuery.Exec(strings.Join(stringSources, ","), target)
		if err != nil {
			return err
		}
		return nil
	}
	insertOutgoingQuery, err := tx.Prepare("UPDATE pages SET outgoing = CASE WHEN outgoing IS NULL THEN '' ELSE outgoing || ',' END || ? WHERE id = ?")
	if err != nil {
		return err
	}
	insertOutgoing := func(source int64, targets []int64) error {
		stringTargets := make([]string, len(targets))
		for i, v := range targets {
			stringTargets[i] = strconv.FormatInt(v, 10)
		}
		_, err := insertOutgoingQuery.Exec(strings.Join(stringTargets, ","), source)
		if err != nil {
			return err
		}
		return nil
	}
	incoming := map[int64][]int64{}
	outgoing := map[int64][]int64{}
	for link := range linkChan {
		incoming[link.target] = append(incoming[link.target], link.source)
		outgoing[link.source] = append(outgoing[link.source], link.target)
		// if memoryLimitReached {
		// 	log.Print("Maximum memory usage exceeded, flushing to database...")
		// 	incomingLength := len(incoming)
		// 	incomingIndex := 0
		// 	for target, sources := range incoming {
		// 		err := insertIncoming(target, sources)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		delete(incoming, target)
		// 		incomingIndex++
		// 		if incomingIndex > incomingLength/2 {
		// 			break
		// 		}
		// 	}
		// 	outgoingLength := len(outgoing)
		// 	outgoingIndex := 0
		// 	for source, targets := range outgoing {
		// 		err := insertOutgoing(source, targets)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		delete(outgoing, source)
		// 		outgoingIndex++
		// 		if outgoingIndex > outgoingLength/2 {
		// 			break
		// 		}
		// 	}
		// }
	}

	log.Print("Flushing incoming links to database...")
	for target, sources := range incoming {
		err := insertIncoming(target, sources)
		if err != nil {
			return err
		}
	}

	log.Print("Flushing outgoing links to database...")
	for source, targets := range outgoing {
		err := insertOutgoing(source, targets)
		if err != nil {
			return err
		}
	}

	return nil
}
