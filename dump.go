package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/cavaliercoder/grab"
)

type DumpFiles struct {
	pageDumpPath  string
	redirDumpPath string
	linkDumpPath  string
	dateString    string
}

type DumpMetadata struct {
	pageDumpHash  *FileHash
	redirDumpHash *FileHash
	linkDumpHash  *FileHash
	dateString    string
}

type FileHash struct {
	name string
	hash []byte
}

// Download the latest 'page.sql.gz', 'pagelinks.sql.gz' and 'redirect.sql.gz' dump files
// to a directory. Accepts a channel over which progress from 0 to 1 will be sent.
func fetchDumpFiles(directory string, mirror string, language *Language, progress chan<- float64) (*DumpFiles, error) {
	meta, err := getLatestDumpMetadata(language)
	if err != nil {
		return nil, err
	}

	// Check existence of mirror and files
	if !httpExists(mirror) {
		return nil, errors.New("mirror does not exist")
	}
	if !httpExists(mirror + "/" + language.Database) {
		return nil, errors.New("mirror does not support specified language")
	}
	if !httpExists(mirror + "/" + language.Database + "/" + meta.dateString) {
		return nil, errors.New("mirror does not contain latest dump")
	}

	// Create grab requests for concurrently downloading the files
	baseUrl := mirror + "/" + language.Database + "/" + meta.dateString
	pageRequest, err := grab.NewRequest(directory, baseUrl+"/"+meta.pageDumpHash.name)
	if err != nil {
		return nil, err
	}
	pageRequest.SetChecksum(sha1.New(), meta.pageDumpHash.hash, true)
	redirRequest, err := grab.NewRequest(directory, baseUrl+"/"+meta.redirDumpHash.name)
	if err != nil {
		return nil, err
	}
	redirRequest.SetChecksum(sha1.New(), meta.redirDumpHash.hash, true)
	linkRequest, err := grab.NewRequest(directory, baseUrl+"/"+meta.linkDumpHash.name)
	if err != nil {
		return nil, err
	}
	linkRequest.SetChecksum(sha1.New(), meta.linkDumpHash.hash, true)

	// Start downloading the files
	client := grab.NewClient()
	pageResponse := client.Do(pageRequest)
	redirResponse := client.Do(redirRequest)
	linkResponse := client.Do(linkRequest)

	// Setup progress reporting
	totalBytes := pageResponse.Size + redirResponse.Size + linkResponse.Size
	go func() {
		for {
			currentBytes := pageResponse.BytesComplete() + redirResponse.BytesComplete() + linkResponse.BytesComplete()
			if currentBytes >= totalBytes {
				return
			}
			progress <- float64(currentBytes) / float64(totalBytes)
			time.Sleep(time.Millisecond * 200)
		}
	}()

	// Check for any request errors
	if err := pageResponse.Err(); err != nil {
		return nil, err
	}
	if err := redirResponse.Err(); err != nil {
		return nil, err
	}
	if err := linkResponse.Err(); err != nil {
		return nil, err
	}

	return &DumpFiles{
		pageDumpPath:  pageResponse.Filename,
		redirDumpPath: redirResponse.Filename,
		linkDumpPath:  linkResponse.Filename,
		dateString:    meta.dateString,
	}, nil
}

// Fetch the latest dump file names and hashes from the official dump
func getLatestDumpMetadata(language *Language) (*DumpMetadata, error) {
	resp, err := http.Get("https://dumps.wikimedia.org/" + language.Database + "/latest/" + language.Database + "-latest-sha1sums.txt")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	checksums, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	pageFileHash, err := findHash(string(checksums), "page.sql.gz")
	if err != nil {
		return nil, err
	}

	redirFileHash, err := findHash(string(checksums), "redirect.sql.gz")
	if err != nil {
		return nil, err
	}

	linkFileHash, err := findHash(string(checksums), "pagelinks.sql.gz")
	if err != nil {
		return nil, err
	}

	return &DumpMetadata{
		pageDumpHash:  pageFileHash,
		redirDumpHash: redirFileHash,
		linkDumpHash:  linkFileHash,
		dateString:    regexp.MustCompile("[0-9]{8}").FindString(pageFileHash.name),
	}, nil
}

// Find a file's hash in a SHA1 checksums file's contents
func findHash(checksums string, filename string) (*FileHash, error) {
	baseRegex := "[0-9a-f]{40}  .+?wiki-[0-9]{8}-"

	fileRegex, err := regexp.Compile(baseRegex + filename)
	if err != nil {
		return nil, err
	}

	info := strings.Split(fileRegex.FindString(checksums), "  ")
	if len(info) != 2 {
		return nil, errors.New(filename + " checksum not found")
	}

	fileName := info[1]
	hexHash := info[0]
	byteHash, err := hex.DecodeString(hexHash)
	if err != nil {
		return nil, err
	}

	return &FileHash{hash: byteHash, name: fileName}, nil
}

// Check whether a HTTP resource exists by sending a HEAD request
func httpExists(url string) bool {
	resp, err := http.Head(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
