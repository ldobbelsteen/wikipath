package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"log"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"
)

const BUFFER_SIZE = 24576

type Page struct {
	id    int64
	title string
}

type Redir struct {
	source int64
	target int64
}

type Link struct {
	source int64
	target int64
}

var titleCleaner = strings.NewReplacer(`\'`, `'`, `_`, ` `)

// Parse the page dump file using a regular expression. It extracts the page_id and page_title columns from the tuples in the dump,
// following the table format from https://www.mediawiki.org/wiki/Manual:Page_table. Only pages in the 0 namespace are accepted.
func pageDumpParse(path string) (<-chan Page, error) {
	output := make(chan Page, BUFFER_SIZE)
	regex := regexp.MustCompile(`\(([0-9]{1,10}),0,'(.{1,255}?)','',[01],[01],[0-9.]+?,'[0-9]+?',(?:'[0-9]+?'|NULL),[0-9]{1,10},[0-9]{1,10},'wikitext',NULL\)`)
	_, err := dumpParse(path, regex, 2048, func(match []string) {
		id := parsePageID(match[0])
		title := titleCleaner.Replace(match[1])
		output <- Page{id, title}
	}, func() {
		close(output)
	})
	if err != nil {
		return nil, err
	}

	return output, nil
}

// Parse the redirect dump file using a regular expression. It extracts the rd_from and rd_title columns from the tuples in the dump,
// following the table format from https://www.mediawiki.org/wiki/Manual:Redirect_table. Only redirects in the 0 namespace are accepted.
// The rd_title is converted to its corresponding ID using a titler map that should be supplied.
func redirDumpParse(path string, titler map[string]int64) (<-chan Redir, error) {
	output := make(chan Redir, BUFFER_SIZE)
	regex := regexp.MustCompile(`\(([0-9]{1,10}),0,'(.{1,255}?)','.{0,32}?','.{0,255}?'\)`)
	_, err := dumpParse(path, regex, 1536, func(match []string) {
		source := parsePageID(match[0])
		if target, titleExists := titler[titleCleaner.Replace(match[1])]; titleExists && source != target {
			output <- Redir{source, target}
		}
	}, func() {
		close(output)
	})
	if err != nil {
		return nil, err
	}

	return output, nil
}

// Parse the link dump file using a regular expression. It extracts the pl_from and pl_title columns from the tuples in the dump,
// following the table format from https://www.mediawiki.org/wiki/Manual:Pagelinks_table. Only links where both the source and target
// namespaces are 0 are accepted. The pl_title is converted to its corresponding ID using a titler map that should be supplied.
// Any redirects in the supplied redirect map are followed such that neither the resulting source nor target are a redirect.
func linkDumpParse(path string, titler map[string]int64, redirects map[int64]int64) (<-chan Link, func(), error) {
	output := make(chan Link, BUFFER_SIZE)
	regex := regexp.MustCompile(`\(([0-9]{1,10}),0,'(.{1,255}?)',0\)`)
	toggle, err := dumpParse(path, regex, 1024, func(match []string) {
		source := parsePageID(match[0])
		if newSource, isRedirect := redirects[source]; isRedirect {
			source = newSource
		}
		if target, titleExists := titler[titleCleaner.Replace(match[1])]; titleExists {
			if newTarget, isRedirect := redirects[target]; isRedirect {
				target = newTarget
			}
			if source != target {
				output <- Link{source, target}
			}
		}
	}, func() {
		close(output)
	})
	if err != nil {
		return nil, nil, err
	}

	return output, toggle, nil
}

// Open a dump file and concurently run a regex on its contents which passes all of the matches to a passed function. It reads
// the file using a buffer, which can cause a regex match to overlap multiple buffers. To solve this, the maximum regex match size
// needs to be passed such that the last n bytes of a buffer will be copied to the start of the next as not to miss any matches. A
// function with which the progress bar can be paused and turned back on is returned. The done function is run when the parsing is done
func dumpParse(path string, regex *regexp.Regexp, maxRegexSize int, output func([]string), done func()) (func(), error) {

	// Open the dump file
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Start a progress bar based on the number of bytes read
	bar := pb.Full.Start64(getFileSize(file))
	reader := bar.NewProxyReader(file)

	// Decompress the gzipped file
	gzip, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	buff := bufio.NewReader(gzip)

	// Start multiple goroutines that will all be running regexes on chunks of the contents of the dump file
	threadCount := runtime.NumCPU()
	textChunks := make(chan string, threadCount*2)
	wait := sync.WaitGroup{}
	for i := 0; i < threadCount; i++ {
		wait.Add(1)
		go func() {
			for chunk := range textChunks {
				result := regex.FindAllStringSubmatch(chunk, -1)
				for _, match := range result {
					output(match[1:])
				}
			}
			wait.Done()
		}()
	}

	// Supply the goroutines with chunks of the file contents in a separate goroutine. This
	// makes sure any overlap between chunks is taken care of by using the max regex size.
	go func() {
		defer done()
		defer bar.Finish()
		defer file.Close()
		defer gzip.Close()
		chunkBuffer := make([]byte, buff.Size()*16+maxRegexSize)
		var lastRead int
		for {
			copy(chunkBuffer, chunkBuffer[lastRead:lastRead+maxRegexSize])
			read, err := buff.Read(chunkBuffer[maxRegexSize:])
			if err != nil {
				if err != io.EOF {
					log.Fatal(err)
				}
				close(textChunks)
				break
			}
			textChunks <- string(chunkBuffer[:maxRegexSize+read])
			lastRead = read
		}
		wait.Wait()
	}()

	var barPaused bool
	return func() {
		if barPaused {
			bar.Start()
			barPaused = false
		} else {
			bar.Finish()
			barPaused = true
		}
	}, nil
}

// Convert a string containing a page ID to its integer represenation. In the dumps, page IDs
// are 10 digit unsigned integers, meaning the max value is 9999999999. This number fits in a
// 34 bit integer, which is why that is set as the bitsize in the parser. Returns 0 on error.
func parsePageID(str string) int64 {
	id, err := strconv.ParseInt(str, 10, 34)
	if err != nil {
		return 0
	}
	return id
}
