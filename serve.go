package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

// Serve a set of databases over the web interface
func serve(databaseDir string, webDir string) error {
	mux := http.NewServeMux()

	// Open the databases in the database directory
	files, err := os.ReadDir(databaseDir)
	if err != nil {
		return err
	}
	databases := map[string]*Database{}
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == DatabaseFileExtension {
			database, err := openDatabase(filepath.Join(databaseDir, file.Name()))
			if err != nil {
				log.Print("failed to open and thus skipping ", file.Name(), ": ", err)
				continue
			}
			databases[database.LanguageCode] = database
		}
	}

	// Add handler for serving web files
	mux.Handle("/", http.FileServer(http.Dir(webDir)))

	// Prepare a list of available databases in JSON marshalled form
	jsonDatabases := func() []byte {
		slc := make([]*Database, 0, len(databases))
		for _, database := range databases {
			slc = append(slc, database)
		}
		marshalled, err := json.Marshal(slc)
		if err != nil {
			log.Fatal("failed to marshal databases: ", err)
		}
		return marshalled
	}()

	// Add handler for serving the list of databases
	mux.HandleFunc("/databases", func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Write(jsonDatabases)
	})

	// Handler for serving the shortest paths between two pages
	mux.HandleFunc("/paths", func(writer http.ResponseWriter, request *http.Request) {
		parameters := request.URL.Query()

		// Extract the URL parameters
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

		// Parse the IDs and return if not valid
		source := parsePageId(sourceRaw)
		if source == 0 {
			http.Error(writer, "source is not a page ID", http.StatusBadRequest)
			return
		}
		target := parsePageId(targetRaw)
		if target == 0 {
			http.Error(writer, "target is not a page ID", http.StatusBadRequest)
			return
		}

		// Get the database corresponding to the language code
		database, supported := databases[languageCode]
		if !supported {
			http.Error(writer, "no database for specified language", http.StatusNotFound)
			return
		}

		// Check if IDs are too large anyways
		if source > database.LargestPageId {
			http.Error(writer, "source ID is too large", http.StatusBadRequest)
			return
		}
		if target > database.LargestPageId {
			http.Error(writer, "target ID is too large", http.StatusBadRequest)
			return
		}

		// Get the shortest paths graph and write it as a response
		err := database.runTransaction(request.Context(), func(tx Transaction) {
			graph, err := tx.getShortestPaths(languageCode, source, target)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					http.Error(writer, "request timeout", http.StatusRequestTimeout)
					return
				} else {
					http.Error(writer, "internal server error", http.StatusInternalServerError)
					log.Print("failed to compute shortest paths graph: ", err)
					return
				}
			}
			marshal, err := json.Marshal(graph)
			if err != nil {
				http.Error(writer, "internal server error", http.StatusInternalServerError)
				log.Print("failed to marshal graph: ", err)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			writer.Write(marshal)
		})
		if err != nil {
			log.Print("error during database transaction: ", err)
		}
	})

	// Start listening on the default port
	log.Print("started listening on port ", ListeningPort)
	return http.ListenAndServe(":"+strconv.Itoa(ListeningPort), mux)
}
