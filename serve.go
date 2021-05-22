package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

//go:embed web/dist
var web embed.FS

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

type Search struct {
	language string
	source   int64
	target   int64
}

func newDatabase(path string, finder LanguageFinder) (*Database, error) {

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

func serve(databaseDir string, finder LanguageFinder, cacheSize int) error {

	// List all files in the database directory
	files, err := os.ReadDir(databaseDir)
	if err != nil {
		return err
	}

	// Open all databases and map from language name to the corresponding database
	databaseList := []*Database{}
	databaseMap := map[string]*Database{}
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == FILE_EXTENSION {
			database, err := newDatabase(filepath.Join(databaseDir, file.Name()), finder)
			if err != nil {
				log.Print("Failed to open ", file.Name(), ": ", err)
				break
			}
			databaseList = append(databaseList, database)
			databaseMap[database.LanguageCode] = database
		}
	}

	// Convert a page ID to the corresponding title
	pageToTitle := func(db *Database, page int64) (string, error) {
		var title string
		err := db.pageToTitleQuery.QueryRow(page).Scan(&title)
		if err != nil {
			return "", err
		}
		return title, nil
	}

	// Convert a page title to the corresponding ID
	titleToPage := func(db *Database, title string) (int64, error) {
		var page int64
		err := db.titleToPageQuery.QueryRow(title).Scan(&page)
		if err != nil {
			return 0, err
		}
		return page, nil
	}

	// Get a random page title from the database
	randomTitle := func(db *Database) string {
		var title string
		for title == "" {
			db.randomTitleQuery.QueryRow().Scan(&title)
		}
		return title
	}

	// Get the incoming links of a page
	getIncoming := func(db *Database, page int64) []int64 {
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
	getOutgoing := func(db *Database, page int64) []int64 {
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
	shortestPaths := func(db *Database, search Search) [][]string {

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
					outgoing := getOutgoing(db, page)
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
					incoming := getIncoming(db, page)
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
					title, err := pageToTitle(db, t)
					if err != nil {
						title = "Error"
					}
					path[i] = title
					t = parents[t]
				}

				// Go down hte chain and get titles
				for i, t := forwardDepth, intersect; i < len(path); i++ {
					title, err := pageToTitle(db, t)
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

	// Handler for serving web files
	web, err := fs.Sub(web, "web/dist")
	if err != nil {
		return err
	}
	http.Handle("/", http.FileServer(http.FS(web)))

	// Handler for serving the list of databases
	encodedDatabaseList, _ := json.Marshal(databaseList)
	http.HandleFunc("/databases", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Write(encodedDatabaseList)
	})

	// Handler for serving a random page title
	http.HandleFunc("/random", func(writer http.ResponseWriter, request *http.Request) {
		language := request.URL.Query().Get("language")
		if language == "" {
			http.Error(writer, "no database language specified", http.StatusBadRequest)
			return
		}

		database, supported := databaseMap[language]
		if !supported {
			http.Error(writer, "no database for specified language", http.StatusNotFound)
			return
		}

		writer.Write([]byte(randomTitle(database)))
	})

	// Handler for serving the shortest paths between two pages
	cache := NewSearchCache(cacheSize)
	http.HandleFunc("/paths", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "max-age=86400")
		parameters := request.URL.Query()

		language := parameters.Get("language")
		if language == "" {
			http.Error(writer, "no database language specified", http.StatusBadRequest)
			return
		}

		sourceTitle := parameters.Get("source")
		if sourceTitle == "" {
			http.Error(writer, "no source page specified", http.StatusBadRequest)
			return
		}

		targetTitle := parameters.Get("target")
		if targetTitle == "" {
			http.Error(writer, "no target page specified", http.StatusBadRequest)
			return
		}

		database, supported := databaseMap[language]
		if !supported {
			http.Error(writer, "no database for specified language", http.StatusNotFound)
			return
		}

		source, err := titleToPage(database, sourceTitle)
		if err != nil {
			http.Error(writer, "source page not found", http.StatusNotFound)
			return
		}

		target, err := titleToPage(database, targetTitle)
		if err != nil {
			http.Error(writer, "target page not found", http.StatusNotFound)
			return
		}

		search := Search{
			language: language,
			source:   source,
			target:   target,
		}

		var paths [][]string
		if cached := cache.Find(search); cached != nil {
			paths = cached
		} else {
			start := time.Now()
			paths = shortestPaths(database, search)
			if time.Since(start).Seconds() > 2 {
				cache.Store(search, paths)
			}
		}

		writer.Header().Set("Content-Type", "application/json")
		json.NewEncoder(writer).Encode(paths)
	})

	// Start listening
	log.Print("Started listening on port ", LISTENING_PORT, "...")
	return http.ListenAndServe(":"+strconv.Itoa(LISTENING_PORT), nil)
}
