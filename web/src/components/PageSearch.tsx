import React, { useEffect, useRef, useState } from "react";
import { toast } from "react-hot-toast";
import { Api, Database, Page } from "../api";
import Dice from "../assets/dice.svg";
import LoadingBlack from "../assets/loading-black.svg";
import { weakStringEquals } from "../misc";
import { Button } from "./generic/Button";
import { InputImage } from "./generic/InputImage";
import { InputText } from "./generic/InputText";

const SEARCH_DEBOUNCE_MS = 400;

export interface SearchState {
  search: string;
  showUnknown: boolean;
  matching:
    | "loading"
    | {
        page?: Page;
        suggestions: Page[];
      };
}

export const defaultSearchState = (): SearchState => ({
  search: "",
  showUnknown: false,
  matching: {
    page: undefined,
    suggestions: [],
  },
});

export const PageSearch = (props: {
  database: Database;
  placeholder: string;
  disabled: boolean;

  state: SearchState | "loadingRandom";
  setState: (v: SearchState | "loadingRandom") => void;
}) => {
  const [inFocus, setInFocus] = useState(false);
  const abort = useRef(new AbortController());
  const debounce = useRef<number>();

  const { setState } = props;

  /** Update search text when not in focus and there is a matching page. */
  useEffect(() => {
    if (
      !inFocus &&
      props.state !== "loadingRandom" &&
      props.state.matching !== "loading" &&
      props.state.matching.page !== undefined &&
      props.state.search !== props.state.matching.page.title
    ) {
      setState({
        ...props.state,
        search: props.state.matching.page.title,
      });
    }
  }, [inFocus, props.state, setState]);

  /** Fetch random page or matching when needed. */
  useEffect(() => {
    abort.current.abort();
    clearTimeout(debounce.current);
    if (props.state === "loadingRandom") {
      const thisAbort = new AbortController();
      abort.current = thisAbort;

      Api.randomPage(props.database.languageCode)
        .then((random) =>
          setState({
            search: random.title,
            showUnknown: false,
            matching: "loading",
          }),
        )
        .catch((err) => {
          if (!thisAbort.signal.aborted) {
            setState({
              search: "",
              showUnknown: false,
              matching: {
                page: undefined,
                suggestions: [],
              },
            });
            toast.error(
              "An unexpected error occurred while getting random page",
            );
            console.error(err);
          }
        });
    } else if (props.state.matching === "loading") {
      const { search } = props.state;

      if (search === "") {
        setState({
          search: "",
          showUnknown: false,
          matching: {
            page: undefined,
            suggestions: [],
          },
        });
        return;
      }

      const thisAbort = new AbortController();
      abort.current = thisAbort;
      debounce.current = setTimeout(
        () =>
          Api.suggestions(
            props.database.languageCode,
            search,
            5,
            thisAbort.signal,
          )
            .then((suggestions) =>
              setState({
                search,
                showUnknown: false,
                matching: {
                  suggestions,
                  page: suggestions.find((page) =>
                    weakStringEquals(page.title, search),
                  ),
                },
              }),
            )
            .catch((err) => {
              if (!thisAbort.signal.aborted) {
                setState({
                  search: "",
                  showUnknown: false,
                  matching: {
                    page: undefined,
                    suggestions: [],
                  },
                });
                toast.error(
                  "An unexpected error occurred while getting page suggestions",
                );
                console.error(err);
              }
            }),
        SEARCH_DEBOUNCE_MS,
      );
    }
  }, [props.state, setState, props.database]);

  return (
    <div className="relative">
      <InputText
        value={props.state !== "loadingRandom" ? props.state.search : ""}
        onChange={(ev) =>
          props.setState({
            search: ev.target.value,
            matching: "loading",
            showUnknown: false,
          })
        }
        onFocus={() => setInFocus(true)}
        onBlur={() => setInFocus(false)}
        placeholder={
          props.state === "loadingRandom"
            ? "Loading random..."
            : props.placeholder
        }
        disabled={props.disabled || props.state === "loadingRandom"}
        invalid={
          props.state !== "loadingRandom" &&
          props.state.matching !== "loading" &&
          props.state.matching.page === undefined &&
          props.state.showUnknown
        }
      />
      {inFocus && props.state !== "loadingRandom" && (
        <div className="absolute left-2 right-2">
          <div className="flex flex-col bg-white rounded">
            {props.state.matching === "loading" ? (
              <span className="text-gray-600 m-1">Loading suggestions...</span>
            ) : props.state.matching.suggestions.length > 0 ? (
              props.state.matching.suggestions.map((suggested, i) => (
                <Button
                  key={i}
                  onMouseDown={() => {
                    props.setState({
                      search: suggested.title,
                      showUnknown: false,
                      matching: "loading",
                    });
                  }}
                  margin="1"
                  padding="0"
                >
                  {suggested.title}
                </Button>
              ))
            ) : (
              props.state.search !== "" && (
                <span className="text-gray-600 m-1">No results found</span>
              )
            )}
          </div>
        </div>
      )}
      <InputImage
        alt="Randomize page"
        src={props.state === "loadingRandom" ? LoadingBlack : Dice}
        disabled={props.disabled || props.state.search === undefined}
        onClick={() => setState("loadingRandom")}
        className="w-6 h-6 absolute right-4 top-0 bottom-0 my-auto p-0 opacity-30 hover:opacity-80"
      />
    </div>
  );
};
