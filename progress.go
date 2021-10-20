package main

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Very simple progress bar supporting stages and elapsed time. Bit hacky, but it's nice.
func newProgress(stages int) (chan<- string, chan<- float64, *sync.WaitGroup) {
	progressChannel := make(chan float64)
	messageChannel := make(chan string)
	var finishWait sync.WaitGroup
	finishWait.Add(1)
	go func() {
		defer finishWait.Done()
		currentMessage := ""
		currentProgress := 0.0
		currentStage := 0
		stageStart := time.Now()
		print := func(percentage bool) {
			if percentage {
				fmt.Printf("\033[2K\rStep %d/%d: %s... %.3f%%", currentStage, stages, currentMessage, currentProgress)
			} else {
				fmt.Printf("\033[2K\rStep %d/%d: %s -> %s", currentStage, stages, currentMessage, time.Since(stageStart).String())
			}
		}
		for {
			select {
			case message := <-messageChannel:

				// Print with elapsed time
				print(false)
				stageStart = time.Now()

				// Update message, reset progress and increment stage
				currentMessage = message
				currentProgress = 0
				currentStage += 1

				// Print based on conditions
				if currentStage > stages {
					fmt.Println()
					fmt.Println(message)
					return
				} else {
					if currentStage > 1 {
						fmt.Println()
					}
					print(true)
				}

			case progress := <-progressChannel:
				currentProgress = progress * 100
				print(true)
			}
		}
	}()
	return messageChannel, progressChannel, &finishWait
}

type ProxyReader struct {
	reader io.Reader
	total  *int64
}

// Create a proxy reader that will increment a number by the number of bytes read.
// This way, the number will represent the total number of read bytes from the reader.
func newProxyReader(reader io.Reader, total *int64) *ProxyReader {
	return &ProxyReader{
		reader: reader,
		total:  total,
	}
}

func (pr *ProxyReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	*pr.total += int64(n)
	return
}
