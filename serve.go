package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Serve a set of databases over a web interface
func serve(dbDir string, webDir string, languages Languages, cacheSize int) error {
	mux := http.NewServeMux()

	// List all files in the database directory
	files, err := os.ReadDir(dbDir)
	if err != nil {
		return err
	}

	// Open all databases and map from language code to the corresponding database
	databases := map[string]*Database{}
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == FILE_EXTENSION {
			database, err := NewDatabase(filepath.Join(dbDir, file.Name()), languages)
			if err != nil {
				log.Print("Failed to open and thus skipping ", file.Name(), ": ", err)
				break
			}
			databases[database.LanguageCode] = database
		}
	}

	// If no databases were found, exit
	if len(databases) == 0 {
		return errors.New("no valid database(s) found")
	}

	// Create search result cache
	cache, err := NewSearchCache(cacheSize)
	if err != nil {
		return err
	}

	// Add handler for serving web files
	mux.Handle("/", http.FileServer(http.Dir(webDir)))

	// Prepare a list of the databases in marshalled form
	marshalledDatabases := func() []byte {
		databaseSlice := []*Database{}
		for _, database := range databases {
			databaseSlice = append(databaseSlice, database)
		}
		marshalled, _ := json.Marshal(databaseSlice)
		return marshalled
	}()

	// Add handler for serving the list of databases
	mux.HandleFunc("/databases", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Write(marshalledDatabases)
	})

	// Add handler for serving a random page title
	mux.HandleFunc("/random", func(writer http.ResponseWriter, request *http.Request) {
		languageCode := request.URL.Query().Get("language")
		if languageCode == "" {
			http.Error(writer, "no language specified", http.StatusBadRequest)
			return
		}
		database, supported := databases[languageCode]
		if !supported {
			http.Error(writer, "no database for specified language", http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		json.NewEncoder(writer).Encode(database.RandomPage())
	})

	// Handler for serving the shortest paths between two pages
	mux.HandleFunc("/paths", func(writer http.ResponseWriter, request *http.Request) {
		parameters := request.URL.Query()

		languageCode := parameters.Get("language")
		if languageCode == "" {
			http.Error(writer, "no language specified", http.StatusBadRequest)
			return
		}
		sourceRaw := parameters.Get("source")
		if sourceRaw == "" {
			http.Error(writer, "no source specified", http.StatusBadRequest)
			return
		}
		targetRaw := parameters.Get("target")
		if targetRaw == "" {
			http.Error(writer, "no target specified", http.StatusBadRequest)
			return
		}

		source := parsePageID(sourceRaw)
		if source == 0 {
			http.Error(writer, "source is not a page ID", http.StatusBadRequest)
			return
		}
		target := parsePageID(targetRaw)
		if target == 0 {
			http.Error(writer, "target is not a page ID", http.StatusBadRequest)
			return
		}

		database, supported := databases[languageCode]
		if !supported {
			http.Error(writer, "no database for specified language", http.StatusNotFound)
			return
		}

		search := Search{source: source, target: target, languageCode: languageCode}
		if cached := cache.Fetch(search); cached != nil {
			writer.Header().Set("Content-Type", "application/json")
			writer.Write(cached)
			return
		}

		start := time.Now()
		graph := database.ShortestPaths(search)
		marshal, _ := json.Marshal(graph)
		writer.Header().Set("Content-Type", "application/json")
		writer.Write(marshal)

		if time.Since(start).Seconds() > 2 {
			cache.Store(search, marshal)
		}
	})

	// Start listening on the default port
	log.Print("Started listening on port ", LISTENING_PORT, "...")
	return http.ListenAndServe(":"+strconv.Itoa(LISTENING_PORT), mux)
}
