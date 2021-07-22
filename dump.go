package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/cavaliercoder/grab"
	"github.com/cheggaaa/pb/v3"
)

type LocalDumpFiles struct {
	pageFilePath  string
	redirFilePath string
	linkFilePath  string
	dateString    string
}

type DumpMetadata struct {
	pageFile   FileHash
	redirFile  FileHash
	linkFile   FileHash
	dateString string
}

type FileHash struct {
	name string
	hash []byte
}

// Download the latest 'page.sql.gz', 'pagelinks.sql.gz' and 'redirect.sql.gz' dump files to a directory
func fetchDumpFiles(directory string, mirror string, language Language) (*LocalDumpFiles, error) {
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, err
	}

	log.Print("Fetching latest dump info...")
	files, err := getLatestDumpInfo(language)
	if err != nil {
		return nil, err
	}

	log.Print("Testing mirror for dump files...")
	if !httpExists(mirror) {
		return nil, errors.New("mirror does not exist")
	}
	if !httpExists(mirror + "/" + language.Database) {
		return nil, errors.New("mirror does not support specified language")
	}
	if !httpExists(mirror + "/" + language.Database + "/" + files.dateString) {
		return nil, errors.New("mirror does not contain latest dump")
	}

	log.Print("Downloading and/or hashing dump files...")
	baseUrl := mirror + "/" + language.Database + "/" + files.dateString
	pageRequest, err := grab.NewRequest(directory, baseUrl+"/"+files.pageFile.name)
	if err != nil {
		return nil, err
	}
	pageRequest.SetChecksum(sha1.New(), files.pageFile.hash, true)
	redirRequest, err := grab.NewRequest(directory, baseUrl+"/"+files.redirFile.name)
	if err != nil {
		return nil, err
	}
	redirRequest.SetChecksum(sha1.New(), files.redirFile.hash, true)
	linkRequest, err := grab.NewRequest(directory, baseUrl+"/"+files.linkFile.name)
	if err != nil {
		return nil, err
	}
	linkRequest.SetChecksum(sha1.New(), files.linkFile.hash, true)

	bar := pb.Full.Start64(0).Set(pb.Bytes, true)
	client := grab.NewClient()
	pageResponse := client.Do(pageRequest)
	redirResponse := client.Do(redirRequest)
	linkResponse := client.Do(linkRequest)
	bar.SetTotal(pageResponse.Size + redirResponse.Size + linkResponse.Size)
	go func() {
		for {
			bar.SetCurrent(pageResponse.BytesComplete() + redirResponse.BytesComplete() + linkResponse.BytesComplete())
			if pageResponse.IsComplete() && redirResponse.IsComplete() && linkResponse.IsComplete() {
				return
			}
			time.Sleep(time.Millisecond * 200)
		}
	}()

	if err := pageResponse.Err(); err != nil {
		return nil, err
	}
	if err := redirResponse.Err(); err != nil {
		return nil, err
	}
	if err := linkResponse.Err(); err != nil {
		return nil, err
	}

	bar.Finish()
	return &LocalDumpFiles{
		pageFilePath:  pageResponse.Filename,
		redirFilePath: redirResponse.Filename,
		linkFilePath:  linkResponse.Filename,
		dateString:    files.dateString,
	}, nil
}

// Fetch the latest dump file names and hashes from the official dump
func getLatestDumpInfo(language Language) (*DumpMetadata, error) {
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
		pageFile:   pageFileHash,
		redirFile:  redirFileHash,
		linkFile:   linkFileHash,
		dateString: regexp.MustCompile("[0-9]{8}").FindString(pageFileHash.name),
	}, nil
}

// Find a file's hash in a SHA1 checksums file's contents
func findHash(checksums string, filename string) (FileHash, error) {
	baseRegex := "[0-9a-f]{40}  .+?wiki-[0-9]{8}-"

	fileRegex, err := regexp.Compile(baseRegex + filename)
	if err != nil {
		return FileHash{}, err
	}

	info := strings.Split(fileRegex.FindString(checksums), "  ")
	if len(info) != 2 {
		return FileHash{}, errors.New(filename + " checksum not found")
	}

	fileName := info[1]
	hexHash := info[0]
	byteHash, err := hex.DecodeString(hexHash)
	if err != nil {
		return FileHash{}, err
	}

	return FileHash{hash: byteHash, name: fileName}, nil
}

// Get the byte size of a file
func getFileSize(file *os.File) int64 {
	info, err := file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// Remove a file if it exists
func deleteFile(path string) error {
	err := os.Remove(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
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
