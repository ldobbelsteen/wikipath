import React, { useRef, useState } from "react";
import { getRandomPage, getSuggestions } from "../helpers/api";
import Autosuggest from "react-autosuggest";
import Dice from "../static/dice.svg";
import { Page } from "../helpers/api";
import { weakStringEquals } from "../helpers/misc";

const theme: Autosuggest.Theme = {
  container: {
    display: "inline-block",
  },
  suggestionsContainer: {
    position: "absolute",
    fontSize: "0.8rem",
    zIndex: 999999,
  },
  suggestionsContainerOpen: {
    backgroundColor: "white",
    width: "calc(100% - 2rem - 2px)",
    border: "1px solid grey",
    padding: "0.5rem",
    margin: "0.5rem",
    top: "3rem",
  },
  suggestionsList: {
    margin: 0,
    padding: 0,
    listStyleType: "none",
  },
  suggestion: {
    cursor: "pointer",
    textAlign: "left",
    fontWeight: "bold",
    borderBottom: "1px solid lightgray",
  },
  suggestionHighlighted: {
    backgroundColor: "lightgray",
  },
};

export default function SearchInput(props: {
  id: string;
  input: string;
  invalid: boolean;
  languageCode: string;
  disabled: boolean;
  placeholder: string;
  setReady: (val: boolean) => void;
  setInput: (val: string) => void;
  setPage: (val: Page | undefined) => void;
}): JSX.Element {
  const [suggestions, setSuggestions] = useState<Page[]>([]);
  const latestFetch = useRef(new AbortController());
  const [randomDisabled, setRandomDisabled] = useState(false);

  // Fetch Wikipedia suggestions with support for abortion
  const updateSuggestions = (search: string) => {
    props.setReady(false);
    latestFetch.current.abort();
    const newController = new AbortController();
    latestFetch.current = newController;
    getSuggestions(search, props.languageCode, 5, newController.signal)
      .then((res) => {
        setSuggestions(res);
        props.setPage(res.find((page) => weakStringEquals(page.title, search)));
        props.setReady(true);
        return;
      })
      .catch((err) => {
        if (err.name !== "AbortError") console.error(err);
      });
  };

  // Fetch a random page from the API
  const randomPage = () => {
    props.setReady(false);
    setRandomDisabled(true);
    getRandomPage(props.languageCode)
      .then((page) => {
        props.setInput(page.title);
        props.setPage(page);
        props.setReady(true);
        setRandomDisabled(false);
        return;
      })
      .catch((err) => console.error(err));
  };

  return (
    <div className="autosuggest">
      <Autosuggest
        id={props.id}
        theme={theme}
        suggestions={suggestions}
        getSuggestionValue={(d) => d.title}
        onSuggestionsFetchRequested={({ value }) => updateSuggestions(value)}
        onSuggestionsClearRequested={() => setSuggestions([])}
        onSuggestionSelected={(_, { suggestion }) => props.setPage(suggestion)}
        renderSuggestion={(suggestion) => <span>{suggestion.title}</span>}
        inputProps={{
          value: props.input,
          disabled: props.disabled || !props.languageCode,
          placeholder: props.placeholder,
          style: props.invalid ? { backgroundColor: "#ff8c8c" } : {},
          onChange: (_, { newValue }) => props.setInput(newValue),
        }}
      />
      <input
        className="random"
        type="image"
        src={Dice}
        alt="Get random page"
        disabled={props.disabled || randomDisabled || !props.languageCode}
        onClick={randomPage}
      ></input>
    </div>
  );
}
