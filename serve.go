package main

import (
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/julienschmidt/httprouter"
)

//nolint
//go:embed web/build
var web embed.FS

// Serve a set of databases over a web interface
func serve(dbDir string, languages Languages, cacheSize int) error {
	router := httprouter.New()

	// List all files in the database directory
	files, err := os.ReadDir(dbDir)
	if err != nil {
		return err
	}

	// Open all databases and map from language code to the corresponding database
	databases := map[string]*Database{}
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == FILE_EXTENSION {
			database, err := NewDatabase(filepath.Join(dbDir, file.Name()), languages, cacheSize)
			if err != nil {
				log.Print("Failed to open and thus skipping ", file.Name(), ": ", err)
				break
			}
			databases[database.LanguageCode] = database
		}
	}
	if len(databases) == 0 {
		return errors.New("no valid database(s) found")
	}

	// Add handler for serving web files
	web, err := fs.Sub(web, "web/build")
	if err != nil {
		return err
	}
	router.ServeFiles("/*filepath", http.FS(web))

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
	router.POST("/databases", func(writer http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
		writer.Header().Set("Content-Type", "application/json")
		_, err := writer.Write(marshalledDatabases)
		if err != nil {
			log.Print("Error writing database list response: ", err)
		}
	})

	// Add handler for serving a random page title
	router.POST("/random/:language", func(writer http.ResponseWriter, _ *http.Request, params httprouter.Params) {
		database, supported := databases[params.ByName("language")]
		if !supported {
			http.Error(writer, "no database for specified language", http.StatusNotFound)
			return
		}

		_, err := writer.Write([]byte(database.RandomTitle()))
		if err != nil {
			log.Print("Error writing random title response: ", err)
		}
	})

	// Handler for serving the shortest paths between two pages
	router.POST("/paths/:language/:source/:target", func(writer http.ResponseWriter, _ *http.Request, params httprouter.Params) {
		database, supported := databases[params.ByName("language")]
		if !supported {
			http.Error(writer, "no database for specified language", http.StatusNotFound)
			return
		}

		source, err := database.TitleToPage(params.ByName("source"))
		if err != nil {
			http.Error(writer, "source page not found", http.StatusNotFound)
			return
		}

		target, err := database.TitleToPage(params.ByName("target"))
		if err != nil {
			http.Error(writer, "target page not found", http.StatusNotFound)
			return
		}

		var result []byte
		search := Search{source: source, target: target}
		if cached := database.CacheGet(search); cached != nil {
			result = cached
		} else {
			start := time.Now()
			paths := database.ShortestPaths(search)
			titles := database.PathsToTitles(paths)
			marshal, _ := json.Marshal(titles)
			if time.Since(start).Seconds() > 2 {
				database.CacheSet(search, result)
			}
			result = marshal
		}

		writer.Header().Set("Content-Type", "application/json")
		_, err = writer.Write(result)
		if err != nil {
			log.Print("Error writing shortest path response: ", err)
		}
	})

	// Start listening
	log.Print("Started listening on port ", LISTENING_PORT, "...")
	return http.ListenAndServe(":"+strconv.Itoa(LISTENING_PORT), router)
}
