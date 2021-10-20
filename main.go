package main

import (
	"flag"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

const ListeningPort = 1789

func main() {

	buildCommand := flag.NewFlagSet("build", flag.ExitOnError)
	buildOutput := buildCommand.String("output", "databases", "Directory to output the database to")
	buildDumps := buildCommand.String("dumps", "dumps", "Directory to download dump files to")
	buildMirror := buildCommand.String("mirror", "https://dumps.wikimedia.org", "Mirror to download dumps from")
	buildLanguage := buildCommand.String("language", "en", "Language to build database of")

	serveCommand := flag.NewFlagSet("serve", flag.ExitOnError)
	serveDatabases := serveCommand.String("databases", "databases", "Parent directory of the database(s) to serve")
	serveWeb := serveCommand.String("web", "web/dist", "Directory of the bundled web files")

	if len(os.Args) < 2 {
		log.Fatal("expected subcommand")
	}

	switch os.Args[1] {
	case "build":

		err := buildCommand.Parse(os.Args[2:])
		if err != nil {
			log.Fatal("failed to parse arguments: ", err)
		}

		err = buildDatabase(*buildOutput, *buildDumps, *buildMirror, *buildLanguage)
		if err != nil {
			log.Fatal("failed to build database: ", err)
		}

	case "serve":

		err := serveCommand.Parse(os.Args[2:])
		if err != nil {
			log.Fatal("failed to parse arguments: ", err)
		}

		err = serve(*serveDatabases, *serveWeb)
		if err != nil {
			log.Fatal("failed to serve databases: ", err)
		}

	default:
		log.Fatal("unknown subcommand")
	}
}
