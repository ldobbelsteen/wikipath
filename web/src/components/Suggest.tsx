import React, { useCallback, useState } from "react";

export const Suggest = <T,>(props: {
  input: string;
  setInput: (s: string) => void;
  placeholder: string;
  disabled: boolean;
  invalid: boolean;

  suggestions: T[];
  suggestionToString: (s: T) => string;
  fetchSuggestions: (s: string) => void;
  clearSuggestions: () => void;
  selectSuggestion: (s: T) => void;
}) => {
  const [inFocus, setInFocus] = useState(false);
  const [debounce, setDebounce] = useState<number>();

  const { fetchSuggestions } = props;

  const fetchSuggestionsWithDebounce = useCallback(
    (input: string) => {
      if (debounce) {
        clearTimeout(debounce);
      }
      const newDebounce = setTimeout(() => {
        fetchSuggestions(input);
      }, 800);
      setDebounce(newDebounce);
    },
    [fetchSuggestions, debounce],
  );

  return (
    <>
      <input
        type="text"
        value={props.input}
        onChange={(ev) => {
          const newInput = ev.target.value;
          props.setInput(newInput);
          if (newInput) {
            fetchSuggestionsWithDebounce(newInput);
          } else {
            props.clearSuggestions();
          }
        }}
        onFocus={() => {
          setInFocus(true);
          if (props.input) {
            props.fetchSuggestions(props.input);
          }
        }}
        onBlur={() => {
          setInFocus(false);
          props.clearSuggestions();
        }}
        placeholder={props.placeholder}
        disabled={props.disabled}
        className={props.invalid ? "bg-red text-white placeholder-white" : ""}
      />
      {inFocus && (
        <div className="absolute left-0 right-0">
          <div className="flex flex-col bg-white rounded">
            {props.suggestions.length > 0 ? (
              props.suggestions.map((s, i) => (
                <button
                  key={i}
                  className="m-0.5 p-0.5"
                  onMouseDown={() => {
                    props.selectSuggestion(s);
                    props.setInput(props.suggestionToString(s));
                  }}
                >
                  {props.suggestionToString(s)}
                </button>
              ))
            ) : (
              <i className="m-0.5 p-0.5">No results found</i>
            )}
          </div>
        </div>
      )}
    </>
  );
};
