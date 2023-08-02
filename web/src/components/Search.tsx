import React, { useCallback, useRef, useState } from "react";
import { toast } from "react-hot-toast";
import { HTTP, Page } from "../api";
import { weakStringEquals } from "../misc";
import Dice from "../static/dice.svg";
import Loading from "../static/loading.svg";
import { Suggest } from "./Suggest";

export const Search = (props: {
  input: string;
  invalid: boolean;
  languageCode?: string;
  disabled: boolean;
  placeholder: string;
  setReady: (v: boolean) => void;
  setInput: (v: string) => void;
  setPage: (v: Page | undefined) => void;
}) => {
  const [suggestions, setSuggestions] = useState<Page[]>([]);
  const suggestionsFetch = useRef(new AbortController());
  const [loadingRandom, setLoadingRandom] = useState(false);

  const { setReady, setPage, languageCode, setInput } = props;

  const clearSuggestions = useCallback(() => {
    suggestionsFetch.current.abort();
    setSuggestions([]);
  }, []);

  const fetchSuggestions = useCallback(
    (search: string) => {
      if (!languageCode) return;
      setReady(false);
      suggestionsFetch.current.abort();
      const controller = new AbortController();
      suggestionsFetch.current = controller;
      HTTP.suggestions(languageCode, search, 5, controller.signal)
        .then((suggestions) => {
          setSuggestions(suggestions);
          setPage(
            suggestions.find((page) => weakStringEquals(page.title, search)),
          );
          return null;
        })
        .finally(() => setReady(true))
        .catch((err) => {
          if (!controller.signal.aborted) {
            clearSuggestions();
            setPage(undefined);
            toast.error(
              "An unexpected error occurred while getting page suggestions :(",
            );
            console.error(err);
          }
        });
    },
    [clearSuggestions, languageCode, setPage, setReady],
  );

  const randomPage = useCallback(() => {
    if (!languageCode) return;
    setReady(false);
    setLoadingRandom(true);
    HTTP.randomPage(languageCode)
      .then((page) => {
        setInput(page.title);
        setPage(page);
        return;
      })
      .finally(() => {
        setReady(true);
        setLoadingRandom(false);
      })
      .catch((err) => {
        setPage(undefined);
        toast.error("An unexpected error occurred while a random page :(");
        console.error(err);
      });
  }, [languageCode, setInput, setPage, setReady]);

  return (
    <div className="relative">
      <Suggest
        input={props.input}
        setInput={props.setInput}
        placeholder={props.placeholder}
        disabled={props.disabled || !props.languageCode}
        invalid={props.invalid}
        suggestions={suggestions}
        suggestionToString={(s) => s.title}
        fetchSuggestions={fetchSuggestions}
        clearSuggestions={clearSuggestions}
        selectSuggestion={props.setPage}
      />
      <input
        className="w-6 h-6 absolute right-2 top-0 bottom-0 my-auto p-0 opacity-30 hover:opacity-80"
        type="image"
        src={loadingRandom ? Loading : Dice}
        alt="Get random page"
        disabled={props.disabled || loadingRandom || !props.languageCode}
        onClick={randomPage}
      />
    </div>
  );
};
