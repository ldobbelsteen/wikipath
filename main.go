package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const FILE_EXTENSION = ".sqlite3"
const LISTENING_PORT = 1789

func main() {

	buildCommand := flag.NewFlagSet("build", flag.ExitOnError)
	buildOutput := buildCommand.String("output", ".", "Directory to output the database to")
	buildDumps := buildCommand.String("dumps", "dumps", "Directory to download dump files to")
	buildMirror := buildCommand.String("mirror", "https://dumps.wikimedia.org", "Mirror to download dumps from")
	buildLanguage := buildCommand.String("language", "en", "Language to build database of")
	buildMemory := buildCommand.Int("memory", 50, "Maximum usage percentage of total system memory")

	serveCommand := flag.NewFlagSet("serve", flag.ExitOnError)
	serveDatabases := serveCommand.String("databases", ".", "Parent directory of the database(s)")
	serveCacheSize := serveCommand.Int("cache", 16384, "The number of searches to keep in cache")

	if len(os.Args) < 2 {
		log.Fatal("expected 'build' or 'serve' subcommands")
	}

	switch os.Args[1] {
	case "build":

		err := buildCommand.Parse(os.Args[2:])
		if err != nil {
			log.Fatal(err)
		}
		start := time.Now()

		finder, err := getLanguages()
		if err != nil {
			log.Fatal(err)
		}

		language, err := finder.Search(*buildLanguage)
		if err != nil {
			log.Fatal(err)
		}

		files, err := fetchDumpFiles(*buildDumps, *buildMirror, language)
		if err != nil {
			log.Fatal(err)
		}

		finalPath := filepath.Join(*buildOutput, language.Database+"-"+files.dateString+FILE_EXTENSION)
		tempPath := finalPath + ".tmp"

		maxMemory := float64(*buildMemory) / 100
		if maxMemory < 0 || maxMemory > 1 {
			log.Fatal(errors.New("specified memory percentage out of bounds"))
		}

		err = buildDatabase(tempPath, files, maxMemory)
		if err != nil {
			log.Fatal(err)
		}

		err = os.Rename(tempPath, finalPath)
		if err != nil {
			log.Fatal(err)
		}

		log.Print("Finished database build, took ", time.Since(start).String(), " total!")

	case "serve":

		err := serveCommand.Parse(os.Args[2:])
		if err != nil {
			log.Fatal(err)
		}

		finder, err := getLanguages()
		if err != nil {
			log.Fatal(err)
		}

		err = serve(*serveDatabases, finder, *serveCacheSize)
		if err != nil {
			log.Fatal(err)
		}

	default:
		log.Fatal("unexpected subcommand, expected 'build' or 'serve'")
	}
}
