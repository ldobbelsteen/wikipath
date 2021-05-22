package main

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

//go:embed web/dist
var web embed.FS

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
			database, err := NewDatabase(filepath.Join(databaseDir, file.Name()), finder)
			if err != nil {
				log.Print("Failed to open ", file.Name(), ": ", err)
				break
			}
			databaseList = append(databaseList, database)
			databaseMap[database.LanguageCode] = database
		}
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

		writer.Write([]byte(database.randomTitle()))
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

		source, err := database.titleToPage(sourceTitle)
		if err != nil {
			http.Error(writer, "source page not found", http.StatusNotFound)
			return
		}

		target, err := database.titleToPage(targetTitle)
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
			paths = database.shortestPaths(search)
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
