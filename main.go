package main

import (
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
	buildMirror := buildCommand.String("mirror", "https://dumps.wikimedia.org", "Mirror to download dumps from")
	buildOutput := buildCommand.String("output", "databases", "Directory to output the database to")
	buildDumps := buildCommand.String("dumps", "dumps", "Directory to download dump files to")
	buildLanguage := buildCommand.String("language", "en", "Language to build database of")
	buildMemory := buildCommand.Int("memory", 20, "Maximum memory usage in gigabytes")

	serveCommand := flag.NewFlagSet("serve", flag.ExitOnError)
	serveDatabases := serveCommand.String("databases", "databases", "Parent directory of the database(s) to serve")
	serveWeb := serveCommand.String("web", "web/dist", "Directory of the bundled web files")
	serveCache := serveCommand.Int("cache", 32, "Maximum search cache size in megabytes")

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

		log.Print("WARNING: maximum memory usage is ", *buildMemory, "GB, make sure the system has this memory available")

		languages, err := GetLanguages()
		if err != nil {
			log.Fatal(err)
		}

		language, err := languages.Search(*buildLanguage)
		if err != nil {
			log.Fatal(err)
		}

		files, err := fetchDumpFiles(*buildDumps, *buildMirror, language)
		if err != nil {
			log.Fatal(err)
		}

		finalPath := filepath.Join(*buildOutput, language.Database+"-"+files.dateString+FILE_EXTENSION)
		tempPath := finalPath + ".tmp"

		err = buildDatabase(tempPath, files, uint64(*buildMemory)*1024*1024*1024)
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

		languages, err := GetLanguages()
		if err != nil {
			log.Fatal(err)
		}

		err = os.MkdirAll(*serveDatabases, 0755)
		if err != nil {
			log.Fatal(err)
		}

		err = os.MkdirAll(*serveWeb, 0755)
		if err != nil {
			log.Fatal(err)
		}

		err = serve(*serveDatabases, *serveWeb, languages, *serveCache*1024*1024)
		if err != nil {
			log.Fatal(err)
		}

	default:
		log.Fatal("unexpected subcommand, expected 'build' or 'serve'")
	}
}
