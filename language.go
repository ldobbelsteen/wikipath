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

type LanguageFinder []Language

// Fetch the list of all the Wikipedia languages and their corresponding codes and database names
// by fetching and parsing a sitematrix from the Wikimedia Commons API.
func GetLanguages() (LanguageFinder, error) {
	resp, err := http.Get("https://commons.wikimedia.org/w/api.php?format=json&action=sitematrix")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	sitematrix := struct {
		RawSites map[string]json.RawMessage `json:"sitematrix"`
	}{}
	err = json.NewDecoder(resp.Body).Decode(&sitematrix)
	if err != nil {
		return nil, err
	}

	languages := []Language{}
	for key, rawSite := range sitematrix.RawSites {
		if key == "specials" || key == "count" {
			continue
		}
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

		for _, subsite := range site.Subsites {
			if strings.Contains(subsite.URL, "wikipedia.org") {
				languages = append(languages, Language{
					Name:     strings.Title(site.Name),
					Code:     site.Code,
					Database: subsite.Dbname,
				})
			}
		}
	}

	return languages, nil
}

// Find a language by a search string. The search string is compared with each of the languages
// available on Wikipedia and the corresponding language is returned.
func (finder LanguageFinder) Search(search string) (Language, error) {
	for _, language := range finder {
		if strings.EqualFold(search, language.Name) || strings.EqualFold(search, language.Code) || strings.EqualFold(search, language.Database) {
			return language, nil
		}
	}
	return Language{}, errors.New("language '" + search + "' not found")
}
