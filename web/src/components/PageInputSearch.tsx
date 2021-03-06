import React, { useRef, useState } from "react";
import Autosuggest from "react-autosuggest";
import { toast } from "react-hot-toast";
import { HTTP, Page } from "../api";
import { weakStringEquals } from "../misc";
import Dice from "../static/dice.svg";
import Loading from "../static/loading.svg";

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

export const PageInputSearch = (props: {
  id: string;
  input: string;
  invalid: boolean;
  languageCode?: string;
  disabled: boolean;
  placeholder: string;
  setReady: (val: boolean) => void;
  setInput: (val: string) => void;
  setPage: (val: Page | undefined) => void;
}) => {
  const [suggestions, setSuggestions] = useState<Page[]>([]);
  const latestFetch = useRef(new AbortController());
  const [randomDisabled, setRandomDisabled] = useState(false);

  /** Fetch Wikipedia suggestions with support for abortion */
  const updateSuggestions = (search: string) => {
    if (props.languageCode) {
      props.setReady(false);
      latestFetch.current.abort();
      const newController = new AbortController();
      latestFetch.current = newController;
      HTTP.getSuggestions(props.languageCode, search, 5, newController.signal)
        .then((suggestions) => {
          setSuggestions(suggestions);
          props.setPage(
            suggestions.find((page) => weakStringEquals(page.title, search))
          );
          return null;
        })
        .finally(() => props.setReady(true))
        .catch((err) => {
          setSuggestions([]);
          props.setPage(undefined);
          toast.error(
            "An unexpected error occurred while getting page suggestions :("
          );
          console.error(err);
        });
    }
  };

  /** Fetch a random page from the API */
  const randomPage = () => {
    if (props.languageCode) {
      props.setReady(false);
      setRandomDisabled(true);
      HTTP.getRandomPage(props.languageCode)
        .then((page) => {
          props.setInput(page.title);
          props.setPage(page);
          return;
        })
        .finally(() => {
          props.setReady(true);
          setRandomDisabled(false);
        })
        .catch((err) => {
          props.setPage(undefined);
          toast.error("An unexpected error occurred while a random page :(");
          console.error(err);
        });
    }
  };

  const random = randomDisabled ? (
    <img className="random" src={Loading} alt="Loading..."></img>
  ) : (
    <input
      className="random"
      type="image"
      src={Dice}
      alt="Get random page"
      disabled={props.disabled || randomDisabled || !props.languageCode}
      onClick={randomPage}
    ></input>
  );

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
      {random}
    </div>
  );
};
