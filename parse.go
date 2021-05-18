package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"
)

const PARSE_BUFFER_SIZE = 24576

// Run a regex on a buffered reader and output the capture groups of all the matches in the reader to a channel. Requires
// a maximum regex size parameter, which is used to make sure any strings overlapping the chunks of the reader aren't missed.
// A smaller maximum regex length is desired, as it decreases the amount of copies between reads, but when it's too small
// it is not guaranteed that no strings are missed. A replacer should also be passed, which is run on every submatch. This can
// be used to cleanup output before passing it into the output channel.
func streamingRegex(reader *bufio.Reader, regex *regexp.Regexp, replacer *strings.Replacer, maxRegexSize int, output chan<- []string, error chan<- error) {
	threadCount := runtime.NumCPU()
	textChunks := make(chan string, threadCount)
	wait := sync.WaitGroup{}

	// Create goroutines listening for chunks of text to regex and output
	for i := 0; i < threadCount; i++ {
		wait.Add(1)
		go func() {
			for chunk := range textChunks {
				result := regex.FindAllStringSubmatch(chunk, -1)
				for _, match := range result {
					out := make([]string, 0, len(match)-1)
					for _, submatch := range match[1:] {
						out = append(out, replacer.Replace(submatch))
					}
					output <- out
				}
			}
			wait.Done()
		}()
	}

	// Read the file chunk by chunk making sure to always have overlap
	// between chunks to make sure regex matches are found on chunk boundaries
	chunkBuffer := make([]byte, reader.Size()*16+maxRegexSize)
	var lastRead int
	for {
		copy(chunkBuffer, chunkBuffer[lastRead:lastRead+maxRegexSize])
		read, err := reader.Read(chunkBuffer[maxRegexSize:])
		if err != nil {
			if err != io.EOF {
				error <- err
			}
			close(textChunks)
			break
		}
		textChunks <- string(chunkBuffer[:maxRegexSize+read])
		lastRead = read
	}

	wait.Wait() // Wait for the channel to empty and the goroutines to finish
	close(output)
	close(error)
}

// Open a dump file and run a multithreaded regex on its contents which passes all of the matches to a function
func dumpParse(path string, regExpr string, maxRegexpSize int, output func([]string) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	bar := pb.Start64(getFileSize(file))
	defer bar.Finish()
	reader := bar.NewProxyReader(file)

	gzip, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer gzip.Close()

	buff := bufio.NewReader(gzip)

	regex, err := regexp.Compile(regExpr)
	if err != nil {
		return err
	}

	match := make(chan []string, PARSE_BUFFER_SIZE)
	error := make(chan error, 1)
	titleCleaner := strings.NewReplacer(`\'`, `'`, `_`, ` `)
	go streamingRegex(buff, regex, titleCleaner, maxRegexpSize, match, error)
	for m := range match {
		err := output(m)
		if err != nil {
			return err
		}
	}
	if err, any := <-error; any {
		return err
	}

	return nil
}
