package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/c2h5oh/datasize"
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
	buildMemory := buildCommand.String("memory", "12GB", "Maximum memory usage")

	serveCommand := flag.NewFlagSet("serve", flag.ExitOnError)
	serveDatabases := serveCommand.String("databases", ".", "Parent directory of the database(s)")
	serveCacheSize := serveCommand.Int("cache", 16384, "The number of searches to keep in cache")

	if len(os.Args) < 2 {
		log.Fatal("expected 'build' or 'serve' subcommands")
	}

	switch os.Args[1] {
	case "build":

		buildCommand.Parse(os.Args[2:])
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

		var maximumMemory datasize.ByteSize
		maximumMemory.UnmarshalText([]byte(*buildMemory))
		err = build(tempPath, files, maximumMemory.Bytes())
		if err != nil {
			log.Fatal(err)
		}

		err = os.Rename(tempPath, finalPath)
		if err != nil {
			log.Fatal(err)
		}

		log.Print("Finished database build, took ", time.Since(start).String(), " total!")

	case "serve":

		serveCommand.Parse(os.Args[2:])

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
