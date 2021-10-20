package main

import (
	"compress/gzip"
	"io"
	"log"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const ChannelBufferSize = 24576
const ReaderBufferSize = 16384

type Page struct {
	id    PageID
	title string
}

type Redir struct {
	source PageID
	target PageID
}

type Link struct {
	source PageID
	target PageID
}

// Parse the page dump file using a regular expression. It extracts the page_id and page_title columns from the tuples in the dump,
// following the table format from https://www.mediawiki.org/wiki/Manual:Page_table. Only pages in the 0 namespace are accepted.
// Output matches to a returned channel which is closed upon completion. Accepts a channel through with current progress between
// 0 and 1 is sent.
func pageDumpParse(path string, progress chan<- float64) (<-chan Page, error) {
	pages := make(chan Page, ChannelBufferSize)
	match := func(match []string) { pages <- Page{parsePageID(match[0]), match[1]} }
	finished := func() { close(pages) }
	regex := regexp.MustCompile(`\(([0-9]{1,10}),0,'(.{1,255}?)','',[01],[01],[0-9.]+?,'[0-9]+?',(?:'[0-9]+?'|NULL),[0-9]{1,10},[0-9]{1,10},'wikitext',NULL\)`)
	return pages, dumpParse(path, regex, 2048, match, finished, progress)
}

// Parse the redirect dump file using a regular expression. It extracts the rd_from and rd_title columns from the tuples in the dump,
// following the table format from https://www.mediawiki.org/wiki/Manual:Redirect_table. Only redirects in the 0 namespace are accepted.
// The rd_title is converted to its corresponding ID using a titler map that should be supplied. Output matches to a returned channel
// which is closed upon completion. Accepts a channel through with current progress between 0 and 1 is sent.
func redirDumpParse(path string, titler map[string]PageID, progress chan<- float64) (<-chan Redir, error) {
	redirs := make(chan Redir, ChannelBufferSize)
	match := func(match []string) {
		source := parsePageID(match[0])
		if target, titleExists := titler[match[1]]; titleExists && source != target {
			redirs <- Redir{source: source, target: target}
		}
	}
	finished := func() { close(redirs) }
	regex := regexp.MustCompile(`\(([0-9]{1,10}),0,'(.{1,255}?)','.{0,32}?','.{0,255}?'\)`)
	return redirs, dumpParse(path, regex, 1536, match, finished, progress)
}

// Parse the link dump file using a regular expression. It extracts the pl_from and pl_title columns from the tuples in the dump,
// following the table format from https://www.mediawiki.org/wiki/Manual:Pagelinks_table. Only links where both the source and target
// namespaces are 0, are accepted. The pl_title is converted to its corresponding ID using a titler map that should be supplied.
// Any redirects in the supplied redirect map are followed such that neither the resulting source nor target are a redirect. Output
// matches to a returned channel which is closed upon completion. Accepts a channel through with current progress between 0 and 1 is sent.
func linkDumpParse(path string, titler map[string]PageID, redirects map[PageID]PageID, progress chan<- float64) (<-chan Link, error) {
	links := make(chan Link, ChannelBufferSize)
	match := func(match []string) {
		source := parsePageID(match[0])
		if newSource, isRedirect := redirects[source]; isRedirect {
			source = newSource
		}
		if target, titleExists := titler[match[1]]; titleExists {
			if newTarget, isRedirect := redirects[target]; isRedirect {
				target = newTarget
			}
			if source != target {
				links <- Link{source, target}
			}
		}
	}
	finished := func() { close(links) }
	regex := regexp.MustCompile(`\(([0-9]{1,10}),0,'(.{1,255}?)',0\)`)
	return links, dumpParse(path, regex, 1024, match, finished, progress)
}

// Open a dump file and concurently run a regex on its contents which passes all of the matches to a passed function. It reads
// the file using a buffer, which can cause a regex match to overlap multiple buffers. To solve this, the maximum regex match size
// needs to be passed such that the last n bytes of a buffer will be copied to the start of the next as not to miss any matches.
// This function should not be used directly, rather use the dump parse functions above.
func dumpParse(path string, regex *regexp.Regexp, maxRegexSize int, output func([]string), finished func(), progress chan<- float64) error {

	// Open the dump file
	file, err := os.Open(path)
	if err != nil {
		return err
	}

	// Get size info of the file
	info, err := file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()

	// Create proxy reader to keep track of read bytes
	var readBytes int64
	proxy := newProxyReader(file, &readBytes)

	// Report back on the number of bytes read as progress
	go func() {
		for {
			if readBytes >= size {
				return
			}
			progress <- float64(readBytes) / float64(size)
			time.Sleep(time.Millisecond * 200)
		}
	}()

	// Decompress the gzipped file
	gzip, err := gzip.NewReader(proxy)
	if err != nil {
		return err
	}

	// Start goroutines for running regex on chunks of contents
	wait := sync.WaitGroup{}
	count := runtime.NumCPU()
	chunks := make(chan string, 2*count)
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func() {
			for chunk := range chunks {
				result := regex.FindAllStringSubmatch(chunk, -1)
				for _, match := range result {
					output(match[1:])
				}
			}
			wait.Done()
		}()
	}

	// Supply the goroutines with text. This makes sure chunk overlap is taken care of.
	go func() {
		defer finished()
		defer file.Close()
		defer gzip.Close()
		buffer := make([]byte, ReaderBufferSize+maxRegexSize)
		var previousReadBytes int
		for {
			copy(buffer, buffer[previousReadBytes:previousReadBytes+maxRegexSize])
			readBytes, err := gzip.Read(buffer[maxRegexSize:])
			if err != nil {
				if err == io.EOF {
					chunks <- string(buffer[:maxRegexSize+readBytes])
					close(chunks)
					break
				} else {
					log.Fatal("fatal error while reading dump file: ", err)
				}
			}
			chunks <- string(buffer[:maxRegexSize+readBytes])
			previousReadBytes = readBytes
		}
		wait.Wait()
	}()

	return nil
}

// Convert a string containing a page ID to its integer represenation. In the dumps, page IDs
// are 10 digit unsigned integers, meaning the max value is 9999999999. This number fits in a
// 34 bit integer. However, to save space, the number is parsed into a 32 bit integer. This
// means any page ID above 4294967296 will not be parsed correctly.
func parsePageID(str string) PageID {
	id, err := strconv.ParseUint(str, 10, 34)
	if err != nil {
		return 0
	}
	return uint32(id)
}
