package main

import (
	"database/sql"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/pbnjay/memory"
)

// Build a new database from scratch using a set of dump files and the desired path of the database
func buildDatabase(path string, files *LocalDumpFiles, maxMemoryFraction float64) error {

	// Delete any previous database remains
	err := deleteFile(path)
	if err != nil {
		return err
	}

	// Create new database and open it
	db, err := sql.Open("sqlite3", "file:"+path+"?_journal=MEMORY")
	if err != nil {
		return err
	}
	defer db.Close()

	// Start a transaction to improve insert performance
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Create the tables. The 'titles' table contains all page IDs and their respective titles. The 'redirects' table
	// contains the source and target page IDs of all redirects. The 'incoming' and 'outgoing' tables contain page IDs
	// with their respective incoming page IDs and outgoing page IDs.
	_, err = tx.Exec(`
		CREATE TABLE titles (
			page_id INTEGER PRIMARY KEY,
			title TEXT NOT NULL
		);
		CREATE TABLE redirects (
			source_id INTEGER PRIMARY KEY,
			target_id INTEGER NOT NULL
		);
		CREATE TABLE incoming (
			target_id INTEGER PRIMARY KEY,
			incoming_ids TEXT NOT NULL
		);
		CREATE TABLE outgoing (
			source_id INTEGER PRIMARY KEY,
			outgoing_ids TEXT NOT NULL
		);
	`)
	if err != nil {
		return err
	}

	// Parse the page dump file and ingest the titles into the database. A map is also created, mapping
	// a page title to its page ID which is useful for the parsing later on.
	log.Print("Parsing & ingesting page dump file...")
	pageChan, err := pageDumpParse(files.pageFilePath)
	if err != nil {
		return err
	}
	titler := map[string]int64{}
	insertTitle, err := tx.Prepare("INSERT OR REPLACE INTO titles VALUES (?, ?)")
	if err != nil {
		return err
	}
	for page := range pageChan {
		titler[page.title] = page.id
		_, err := insertTitle.Exec(page.id, page.title)
		if err != nil {
			return err
		}
	}

	// Parse the redirects dump file and create a map that maps a page ID to the page ID it redirects to
	log.Print("Parsing redirect dump file...")
	redirChan, err := redirDumpParse(files.redirFilePath, titler)
	if err != nil {
		return err
	}
	redirects := map[int64]int64{}
	for redirect := range redirChan {
		redirects[redirect.source] = redirect.target
	}

	// Loop over the redirects map and update the targets of redirects that have another redirect as a target. This also makes sure to break
	// any cyclic redirects, favoring the deepest chain of redirects before a cycle occurs. Cyclic redirects should only occur when dumps are
	// created in the middle of page edits where titles are changed causing redirects to be messed up, which is very rare. All targets in the
	// map are now guaranteed to not be redirects themselves. The redirects are inserted into the database.
	log.Print("Cleaning up & ingesting redirects...")
	insertRedirect, err := tx.Prepare("INSERT INTO redirects VALUES (?, ?)")
	if err != nil {
		return err
	}
	bar := pb.Full.Start(len(redirects))
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
		_, err := insertRedirect.Exec(source, target)
		if err != nil {
			return err
		}
	}
	bar.Finish()

	// Parse the pagelinks dump file and store the incoming and outgoing links for all page IDs in
	// a large cache. The cache is in-memory and can grow to be quite large. At the end all of the
	// incoming and outgoing links are inserted into the database as text, where the IDs are delimited
	// by a |. If the maximum memory usage is exceeded by the cache, it flushes part of its contents to
	// free up some space. This intermediate flushing is however not desirable, as it causes more inserts
	// to be needed.
	log.Print("Parsing link dump file...")
	linkChan, toggleProgress, err := linkDumpParse(files.linkFilePath, titler, redirects)
	if err != nil {
		return err
	}
	incoming := map[int64][]int64{}
	outgoing := map[int64][]int64{}
	incomingInsert, err := tx.Prepare("INSERT INTO incoming VALUES (?, ?) ON CONFLICT DO UPDATE SET incoming_ids = incoming_ids || '|' || ?")
	if err != nil {
		return err
	}
	outgoingInsert, err := tx.Prepare("INSERT INTO outgoing VALUES (?, ?) ON CONFLICT DO UPDATE SET outgoing_ids = outgoing_ids || '|' || ?")
	if err != nil {
		return err
	}
	flushCache := func(fraction float64) error {
		targetTotalSize := int((1 - fraction) * float64(len(incoming)+len(outgoing)))
		incomingFlush := len(incoming) - targetTotalSize/2
		outgoingFlush := len(outgoing) - targetTotalSize/2
		bar := pb.Full.Start(incomingFlush + outgoingFlush)
		toSeparatedString := func(slc []int64) string {
			stringified := make([]string, len(slc))
			for index, source := range slc {
				stringified[index] = strconv.FormatInt(source, 10)
			}
			return strings.Join(stringified, "|")
		}
		flushIndex := 0
		for target, sources := range incoming {
			separated := toSeparatedString(sources)
			_, err := incomingInsert.Exec(target, separated, separated)
			if err != nil {
				return err
			}
			delete(incoming, target)
			flushIndex++
			bar.Increment()
			if flushIndex >= incomingFlush {
				break
			}
		}
		flushIndex = 0
		for source, targets := range outgoing {
			separated := toSeparatedString(targets)
			_, err := outgoingInsert.Exec(source, separated, separated)
			if err != nil {
				return err
			}
			delete(outgoing, source)
			flushIndex++
			bar.Increment()
			if flushIndex >= outgoingFlush {
				break
			}
		}
		bar.Finish()
		return nil
	}
	maxMemoryBytes := uint64(float64(memory.TotalMemory()) * maxMemoryFraction)
	exceedingMemory := false
	go func() {
		var info runtime.MemStats
		for {
			time.Sleep(time.Second)
			runtime.ReadMemStats(&info)
			exceedingMemory = info.Alloc >= maxMemoryBytes
		}
	}()
	for link := range linkChan {
		incoming[link.target] = append(incoming[link.target], link.source)
		outgoing[link.source] = append(outgoing[link.source], link.target)
		if exceedingMemory {
			toggleProgress()
			log.Print("Maximum memory usage exceeded, partially ingesting into database...")
			err := flushCache(0.4)
			if err != nil {
				return err
			}
			exceedingMemory = false
			log.Print("Continuing parsing...")
			toggleProgress()
		}
	}
	log.Print("Ingesting links into database...")
	err = flushCache(1.0)
	if err != nil {
		return err
	}

	// Create index to optimize getting a page ID by a page's title
	log.Print("Creating title finding optimization index...")
	_, err = tx.Exec("CREATE INDEX titler ON titles (title)")
	if err != nil {
		return err
	}

	// Commit the entire transaction
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
