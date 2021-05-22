package main

import (
	"database/sql"
	"log"

	"github.com/cheggaaa/pb/v3"
)

// Build a new database from scratch using a set of dump files and the target path. The resulting database has two main tables,
// the 'pages' and 'links' tables. The pages table contains the ID, title and redirect for every page in the dump. The links table
// contains the source ID and target ID of every single link in the dump.
//
// The build process guarantees the following properties on the pages table:
// - The ID column is the primary key and thus unique
// - Finding a page ID by a page title is indexed and thus O(log n)
// - The redirect column contains a 0 if the page is NOT a redirect
// - The IDs in the redirect column are NOT themselves a redirect
//
// The build process guarantees the following properties on the links table:
// - There is no column-based index meaning duplicate links may exist
// - The IDs in the source column are NOT redirects
// - The IDs in the target column are NOT redirects
// - Finding all targets given a source is indexed and thus O(log n)
// - Finding all sources given a target is indexed and thus O(log n)
func build(path string, files LocalDumpFiles) error {

	// Delete any previous database
	err := deleteFile(path)
	if err != nil {
		return err
	}

	// Create new database and open with full privileges
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

	// Create the table structure. The redirect column contains the ID a page redirects to. If it is 0 the page is
	// not a redirect. There is no primary key in the links table in order to improve insert performance, because no index
	// will need to be maintained. Indices are created afterwards for query performance.
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

	// Parse the page dump file using a regular expression. It extracts the page_id and page_title columns from the tuples in the dump, following the table
	// format from https://www.mediawiki.org/wiki/Manual:Page_table. Only pages in the 0 namespace are accepted. The pages are inserted into the database
	// with redirect 0 for now, because the redirects will be inserted/updated later. A map is also created, mapping a page title to a page ID.
	log.Print("Parsing & ingesting page dump file...")
	expression := `\(([0-9]{1,10}),0,'(.{1,255}?)','',[01],[01],[0-9.]+?,'[0-9]+?',(?:'[0-9]+?'|NULL),[0-9]{1,10},[0-9]{1,10},'wikitext',NULL\)`
	titler := map[string]Page{}
	insertPage, err := tx.Prepare("INSERT OR REPLACE INTO pages VALUES (?, ?, 0)")
	if err != nil {
		return err
	}
	err = dumpParse(files.pageFilePath, expression, 2048, func(match []string) error {
		id, err := parsePageID(match[0])
		if err != nil {
			return err
		}
		title := match[1]

		titler[title] = id
		_, err = insertPage.Exec(id, title)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Create index to optimize getting a page ID by a page's title
	log.Print("Creating title finding optimization index...")
	_, err = tx.Exec("CREATE INDEX titler ON pages (title)")
	if err != nil {
		return err
	}

	// Parse the redirect dump file using a regular expression. It extracts the rd_from and rd_title columns from the tuples in the dump, following
	// the table format from https://www.mediawiki.org/wiki/Manual:Redirect_table. Only redirects in the 0 namespace are accepted. The rd_title is
	// converted to its corresponding ID and a map that maps a page ID to the page ID it redirects to is created.
	log.Print("Parsing redirect dump file...")
	expression = `\(([0-9]{1,10}),0,'(.{1,255}?)','.{0,32}?','.{0,255}?'\)`
	redirects := map[Page]Page{}
	err = dumpParse(files.redirFilePath, expression, 1536, func(match []string) error {
		source, err := parsePageID(match[0])
		if err != nil {
			return err
		}

		// Only accept if the target exists and is not the same as the source
		if target, targetExists := titler[match[1]]; targetExists && source != target {
			redirects[source] = target
		}

		return nil
	})
	if err != nil {
		return err
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
			encountered := []Page{target} // Keep track of followed redirects
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
		}
		_, err := updateRedirect.Exec(target, source)
		if err != nil {
			return err
		}
	}
	bar.Finish()

	// Parse the link dump file using a regular expression. It extracts the pl_from and pl_title columns from the tuples in the dump, following the
	// table format from https://www.mediawiki.org/wiki/Manual:Pagelinks_table. Only links where both the source and target namespaces are 0 are
	// accepted. The pl_title is converted to its corresponding ID and the link is inserted into the database.
	log.Print("Parsing & ingesting link dump file...")
	insertLink, err := tx.Prepare("INSERT INTO links VALUES (?, ?)")
	if err != nil {
		return err
	}
	expression = `\(([0-9]{1,10}),0,'(.{1,255}?)',0\)`
	err = dumpParse(files.linkFilePath, expression, 1024, func(match []string) error {
		source, err := parsePageID(match[0])
		if err != nil {
			return err
		}

		// Follow any redirect for the source
		newSource, isRedirect := redirects[source]
		if isRedirect {
			source = newSource
		}

		if target, targetExists := titler[match[1]]; targetExists {

			// Follow any redirect for the target
			newTarget, isRedirect := redirects[target]
			if isRedirect {
				target = newTarget
			}

			if source != target {
				_, err := insertLink.Exec(source, target)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
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
