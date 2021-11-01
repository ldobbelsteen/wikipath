package main

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const DatabaseFileExtension = ".sqlite3"
const TemporaryFileExtension = ".tmp"
const sqliteCacheSizeMegabytes = 256

// Build a database from scratch. This handles determining the language, downloading the dumps, parsing the dumps
// and ingesting them into a database. Includes helpful logging to terminal indicating what is happening.
func buildDatabase(databaseDir string, dumpsDir string, dumpMirror string, rawLanguage string) error {
	messages, progresses, progressWait := newProgress(8)
	start := time.Now()

	// Find the Wikipedia language corresponding to the passed language
	language, err := getLanguage(rawLanguage)
	if err != nil {
		return err
	}

	// Create the directory for storing the dump files if it doesn't exist
	err = os.MkdirAll(dumpsDir, 0755)
	if err != nil {
		return err
	}

	// Download the dump files of the language from the mirror
	messages <- "Downloading and/or hashing dump files"
	dumpFiles, err := fetchDumpFiles(dumpsDir, dumpMirror, language, progresses)
	if err != nil {
		return err
	}

	// Determine temporary path and remove any previous leftovers
	path := filepath.Join(databaseDir, language.Database+"-"+dumpFiles.dateString+DatabaseFileExtension+TemporaryFileExtension)
	err = os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Create a new database file
	cacheBytes := strconv.Itoa(sqliteCacheSizeMegabytes * 1024 * 1024)
	database, err := sql.Open("sqlite3", "file:"+path+"?_journal=OFF&_sync=OFF&_locking=EXCLUSIVE&_cache_size="+cacheBytes)
	if err != nil {
		os.Remove(path)
		return err
	}
	defer database.Close()

	// Start transaction
	tx, err := database.Begin()
	if err != nil {
		os.Remove(path)
		return err
	}

	// Create the tables
	_, err = tx.Exec(`
		CREATE TABLE metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);

		CREATE TABLE redirects (
			id INTEGER PRIMARY KEY,
			redirect INTEGER NOT NULL
		);

		CREATE TABLE incoming (
			id INTEGER PRIMARY KEY,
			incoming BLOB NOT NULL
		);

		CREATE TABLE outgoing (
			id INTEGER PRIMARY KEY,
			outgoing BLOB NOT NULL
		);
	`)
	if err != nil {
		os.Remove(path)
		return err
	}

	// Prepare all of the insertion statements
	insertMetadata, err := tx.Prepare("INSERT INTO metadata VALUES (?, ?)")
	if err != nil {
		os.Remove(path)
		return err
	}
	insertRedirect, err := tx.Prepare("INSERT INTO redirects VALUES (?, ?)")
	if err != nil {
		os.Remove(path)
		return err
	}
	insertIncoming, err := tx.Prepare("INSERT INTO incoming VALUES (?, ?)")
	if err != nil {
		os.Remove(path)
		return err
	}
	insertOutgoing, err := tx.Prepare("INSERT INTO outgoing VALUES (?, ?)")
	if err != nil {
		os.Remove(path)
		return err
	}

	// Parse the page dump file. Creates a map mapping from a page's title to
	// the corresponding page ID. Also determines the largest page ID.
	var maxPageID PageID
	titler := map[string]PageID{}
	messages <- "Parsing page dump file"
	pageChan, err := pageDumpParse(dumpFiles.pageDumpPath, progresses)
	if err != nil {
		os.Remove(path)
		return err
	}
	for page := range pageChan {
		titler[page.title] = page.id
		if page.id > maxPageID {
			maxPageID = page.id
		}
	}

	// Insert database metadata
	_, err = insertMetadata.Exec("dumpDate", dumpFiles.dateString)
	if err != nil {
		os.Remove(path)
		return err
	}
	_, err = insertMetadata.Exec("buildDate", time.Now().Format("20060102"))
	if err != nil {
		os.Remove(path)
		return err
	}
	_, err = insertMetadata.Exec("langCode", language.Code)
	if err != nil {
		os.Remove(path)
		return err
	}
	_, err = insertMetadata.Exec("langName", language.Name)
	if err != nil {
		os.Remove(path)
		return err
	}
	_, err = insertMetadata.Exec("maxPageID", strconv.FormatUint(uint64(maxPageID), 10))
	if err != nil {
		os.Remove(path)
		return err
	}

	// Parse the redirect dump file. Creates a map that maps a page ID to the page ID it redirects to.
	messages <- "Parsing redirects dump file"
	redirects := map[PageID]PageID{}
	redirChan, err := redirDumpParse(dumpFiles.redirDumpPath, titler, progresses)
	if err != nil {
		os.Remove(path)
		return err
	}
	for redirect := range redirChan {
		redirects[redirect.source] = redirect.target
	}

	// Loop over the redirects map and update the targets of redirects that have another redirect as a target. This also makes sure to break
	// any cyclic redirects, favoring the deepest chain of redirects before a cycle occurs. Cyclic redirects should only occur when dumps are
	// created in the middle of page edits where titles are changed causing redirects to be messed up, which is very rare. All targets in the
	// map are now guaranteed to not be redirects themselves. The redirects are inserted into the database.
	messages <- "Cleaning up and ingesting redirects"
	totalRedirects := len(redirects)
	currentRedirects := 0
	go func() {
		for {
			if currentRedirects >= totalRedirects {
				return
			}
			progresses <- float64(currentRedirects) / float64(totalRedirects)
			time.Sleep(time.Millisecond * 200)
		}
	}()
	for source, target := range redirects {
		currentRedirects += 1
		if _, targetIsRedir := redirects[target]; targetIsRedir {
			encountered := []PageID{target} // Keep track of followed redirects
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
			os.Remove(path)
			return err
		}
	}

	// Parse the pagelink dump file and store the incoming and outgoing links for all page IDs in large maps
	messages <- "Parsing pagelink dump file"
	incoming := make(map[PageID][]PageID)
	outgoing := make(map[PageID][]PageID)
	linkChan, err := linkDumpParse(dumpFiles.linkDumpPath, titler, redirects, progresses)
	if err != nil {
		os.Remove(path)
		return err
	}
	for link := range linkChan {
		incoming[link.target] = append(incoming[link.target], link.source)
		outgoing[link.source] = append(outgoing[link.source], link.target)
	}

	// Ingest incoming links into the database
	messages <- "Ingesting incoming links"
	currentIncoming := 0
	totalIncoming := len(incoming)
	go func() {
		for {
			if currentIncoming >= totalIncoming {
				return
			}
			progresses <- float64(currentIncoming) / float64(totalIncoming)
			time.Sleep(time.Millisecond * 200)
		}
	}()
	for page, inc := range incoming {
		_, err = insertIncoming.Exec(page, pagesToBytes(inc))
		if err != nil {
			os.Remove(path)
			return err
		}
		delete(incoming, page)
		currentIncoming += 1
	}

	// Ingest outgoing links into the database
	messages <- "Ingesting outgoing links"
	currentOutgoing := 0
	totalOutgoing := len(outgoing)
	go func() {
		for {
			if currentOutgoing >= totalOutgoing {
				return
			}
			progresses <- float64(currentOutgoing) / float64(totalOutgoing)
			time.Sleep(time.Millisecond * 200)
		}
	}()
	for page, out := range outgoing {
		_, err = insertOutgoing.Exec(page, pagesToBytes(out))
		if err != nil {
			os.Remove(path)
			return err
		}
		delete(outgoing, page)
		currentOutgoing += 1
	}

	messages <- "Finishing up"

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		os.Remove(path)
		return err
	}

	// Rename the database without the postfix
	newPath := strings.TrimSuffix(path, TemporaryFileExtension)
	err = os.Remove(newPath)
	if err != nil && !os.IsNotExist(err) {
		os.Remove(path)
		return err
	}
	err = os.Rename(path, newPath)
	if err != nil {
		os.Remove(path)
		return err
	}

	messages <- "Finished database build, took " + time.Since(start).String() + "!"
	progressWait.Wait()
	return nil
}

// Convert a slice of page IDs to a concatenated slice
// of their corresponding byte representations. Ignores
// any duplicates in the slice.
func pagesToBytes(ps []PageID) []byte {
	length := len(ps)
	duplicateCount := 0
	set := make(map[PageID]struct{})
	buffer := make([]byte, length*4)
	for i := 0; i < length; i++ {
		page := ps[i]
		if _, alreadyExists := set[page]; !alreadyExists {
			view := buffer[(i-duplicateCount)*4 : (i-duplicateCount+1)*4]
			binary.LittleEndian.PutUint32(view, page)
			set[page] = struct{}{}
		} else {
			duplicateCount += 1
		}
	}
	return buffer[:4*(length-duplicateCount)]
}

// Convert a byte representation to its page ID.
func bytesToPage(b []byte) (PageID, error) {
	if len(b) != 4 {
		return 0, errors.New("invalid page bytes length")
	}
	return binary.LittleEndian.Uint32(b), nil
}

// Convert a concatenated slice of byte representations of page IDs back to a slice of page IDs
func bytesToPages(bs []byte) ([]PageID, error) {
	if len(bs)%4 != 0 {
		return nil, errors.New("invalid pages bytes length")
	}
	length := len(bs) / 4
	result := make([]PageID, length)
	for i := 0; i < length; i++ {
		page, err := bytesToPage(bs[i*4 : (i+1)*4])
		if err != nil {
			return nil, err
		}
		result[i] = page
	}
	return result, nil
}
