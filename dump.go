package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/cheggaaa/pb/v3"
)

type LocalDumpFiles struct {
	pageFilePath  string
	redirFilePath string
	linkFilePath  string
	dateString    string
}

type DumpFilesInfo struct {
	pageFile   Hash
	redirFile  Hash
	linkFile   Hash
	dateString string
}

type Hash struct {
	name string
	hash string
}

// Download the latest 'page.sql.gz', 'pagelinks.sql.gz' and 'redirect.sql.gz' dump files to a directory
// and return their paths. The directory to download to, the mirror to download from, the language and
// date of the dump download should be specified.
func fetchDumpFiles(directory string, mirror string, language Language) (LocalDumpFiles, error) {
	files, err := getLatestFileInfo(language)
	if err != nil {
		return LocalDumpFiles{}, err
	}

	if !httpExists(mirror) {
		return LocalDumpFiles{}, errors.New("mirror does not exist")
	}
	if !httpExists(mirror + "/" + language.Database) {
		return LocalDumpFiles{}, errors.New("mirror does not support language")
	}
	if !httpExists(mirror + "/" + language.Database + "/" + files.dateString) {
		return LocalDumpFiles{}, errors.New("mirror does not support latest dump date")
	}

	baseUrl := mirror + "/" + language.Database + "/" + files.dateString
	localFiles := LocalDumpFiles{
		pageFilePath:  filepath.Join(directory, files.pageFile.name),
		redirFilePath: filepath.Join(directory, files.redirFile.name),
		linkFilePath:  filepath.Join(directory, files.linkFile.name),
		dateString:    files.dateString,
	}

	err = os.MkdirAll(directory, 0755)
	if err != nil {
		return LocalDumpFiles{}, err
	}

	err = downloadFile(localFiles.pageFilePath, baseUrl+"/"+files.pageFile.name, files.pageFile.hash)
	if err != nil {
		return LocalDumpFiles{}, err
	}

	err = downloadFile(localFiles.redirFilePath, baseUrl+"/"+files.redirFile.name, files.redirFile.hash)
	if err != nil {
		return LocalDumpFiles{}, err
	}

	err = downloadFile(localFiles.linkFilePath, baseUrl+"/"+files.linkFile.name, files.linkFile.hash)
	if err != nil {
		return LocalDumpFiles{}, err
	}

	return localFiles, nil
}

// Fetch the latest dump file info from the official dump
func getLatestFileInfo(language Language) (DumpFilesInfo, error) {
	resp, err := http.Get("https://dumps.wikimedia.org/" + language.Database + "/latest/" + language.Database + "-latest-sha1sums.txt")
	if err != nil {
		return DumpFilesInfo{}, err
	}
	defer resp.Body.Close()

	checksums, err := io.ReadAll(resp.Body)
	if err != nil {
		return DumpFilesInfo{}, err
	}

	pageFileHash, err := findHash(string(checksums), "page.sql.gz")
	if err != nil {
		return DumpFilesInfo{}, err
	}

	redirFileHash, err := findHash(string(checksums), "redirect.sql.gz")
	if err != nil {
		return DumpFilesInfo{}, err
	}

	linkFileHash, err := findHash(string(checksums), "pagelinks.sql.gz")
	if err != nil {
		return DumpFilesInfo{}, err
	}

	return DumpFilesInfo{
		pageFile:   pageFileHash,
		redirFile:  redirFileHash,
		linkFile:   linkFileHash,
		dateString: regexp.MustCompile("[0-9]{8}").FindString(pageFileHash.name),
	}, nil
}

// Find a file's hash in the SHA1 checksums file's contents
func findHash(checksums string, filename string) (Hash, error) {
	baseRegex := "[0-9a-f]{40}  .+?wiki-[0-9]{8}-"

	fileRegex, err := regexp.Compile(baseRegex + filename)
	if err != nil {
		return Hash{}, err
	}

	info := strings.Split(fileRegex.FindString(checksums), "  ")
	if len(info) != 2 {
		return Hash{}, errors.New(filename + " checksum not found")
	}

	return Hash{hash: info[0], name: info[1]}, nil
}

// Download a file from a URL to a target file path and confirm hash
func downloadFile(target string, url string, sha1 string) error {
	base := filepath.Base(target)

	if _, err := os.Stat(target); err == nil {
		log.Print("Found existing ", base, " file, confirming hash...")
		hash, err := getFileSha1Hash(target)
		if err != nil {
			return err
		}
		if hash == sha1 {
			return nil
		}
		log.Print("Hashes don't match, downloading again...")
	}

	log.Print("Downloading ", base, " file...")

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return err
	}

	bar := pb.Start64(size)
	defer bar.Finish()
	reader := bar.NewProxyReader(resp.Body)

	output, err := os.Create(target)
	if err != nil {
		return err
	}
	defer output.Close()

	_, err = io.Copy(output, reader)
	if err != nil {
		return err
	}

	bar.Finish()
	log.Print("Confirming hash for ", base, " file...")

	hash, err := getFileSha1Hash(target)
	if err != nil {
		return err
	}
	if hash != sha1 {
		return errors.New("downloaded file has incorrect hash")
	}

	return nil
}

// Get the SHA1 hash of a file in hexadecimal encoding
func getFileSha1Hash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	bar := pb.Start64(getFileSize(file))
	defer bar.Finish()
	reader := bar.NewProxyReader(file)

	hash := sha1.New()
	_, err = io.Copy(hash, reader)
	if err != nil {
		return "", err
	}

	hexa := hex.EncodeToString(hash.Sum(nil))
	return hexa, nil
}

// Get the byte size of a file
func getFileSize(file *os.File) int64 {
	info, err := file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
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
