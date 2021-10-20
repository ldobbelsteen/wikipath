package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type Language struct {
	Name     string
	Code     string
	Database string
}

// Search a language by a search string by looking for it in
// a sitematrix from the Wikimedia Commons API.
func getLanguage(search string) (*Language, error) {

	// Fetch the sitematrix
	resp, err := http.Get("https://commons.wikimedia.org/w/api.php?format=json&action=sitematrix")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Partially decode into JSON
	sitematrix := struct {
		RawSites map[string]json.RawMessage `json:"sitematrix"`
	}{}
	err = json.NewDecoder(resp.Body).Decode(&sitematrix)
	if err != nil {
		return nil, err
	}

	// Loop over all websites and find the language
	for key, rawSite := range sitematrix.RawSites {
		if key == "specials" || key == "count" {
			continue
		}

		// Decode the site
		site := struct {
			Code     string `json:"code"`
			Name     string `json:"name"`
			Subsites []struct {
				URL    string `json:"url"`
				Dbname string `json:"dbname"`
			} `json:"site"`
		}{}
		err = json.Unmarshal(rawSite, &site)
		if err != nil {
			return nil, err
		}

		// Check for Wikipedia subsites and compare if found
		for _, subsite := range site.Subsites {
			if strings.Contains(subsite.URL, "wikipedia.org") {
				language := Language{
					Name:     strings.Title(site.Name),
					Code:     site.Code,
					Database: subsite.Dbname,
				}
				if strings.EqualFold(search, language.Name) || strings.EqualFold(search, language.Code) || strings.EqualFold(search, language.Database) {
					return &language, nil
				}
			}
		}
	}

	return nil, errors.New("language '" + search + "' not found")
}
