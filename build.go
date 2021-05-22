package main

import (
	"database/sql"
	"log"
	"os"

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

	// Create the table structure. The 'pages' table represents all the pages by its ID, title and the page it
	// redirects to. The redirect column is 0 if the page is not a redirect and the ID of the target page
	// if it is. The 'links' table represents all inter-article hyperlinks. The table is not indexed for
	// performance reasons. Indices are created at the end of the build process for query performance.
	_, err = tx.Exec(`
		CREATE TABLE pages (
			id INTEGER PRIMARY KEY,
			title TEXT NOT NULL,
			redirect INTEGER NOT NULL
		);
		CREATE TABLE links (
			source INTEGER NOT NULL,
			target INTEGER NOT NULL
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
	insertPage, err := tx.Prepare("INSERT OR REPLACE INTO pages VALUES (?, ?, 0)")
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

	// Parse the pagelinks dump file and insert the links into the database
	log.Print("Parsing & ingesting link dump file...")
	linkChan := make(chan Link, BUFFER_SIZE)
	go linkDumpParse(files.linkFilePath, titler, redirects, linkChan)
	insertLink, err := tx.Prepare("INSERT INTO links VALUES (?, ?)")
	if err != nil {
		return err
	}
	for link := range linkChan {
		_, err := insertLink.Exec(link.source, link.target)
		if err != nil {
			return err
		}
	}

	// Create index to optimize getting pages that link to a page
	log.Print("Creating incoming links optimization index...")
	_, err = tx.Exec("CREATE INDEX incoming ON links (source)")
	if err != nil {
		return err
	}

	// Create index to optimize getting pages that a page links to
	log.Print("Creating outgoing links optimization index...")
	_, err = tx.Exec("CREATE INDEX outgoing ON links (target)")
	if err != nil {
		return err
	}

	return nil
}
