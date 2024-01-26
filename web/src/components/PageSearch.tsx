import React, { useCallback, useEffect, useRef, useState } from "react";
import { toast } from "react-hot-toast";
import { Api, Database, Page } from "../api";
import { weakStringEquals } from "../misc";
import Dice from "../static/dice.svg";
import LoadingBlack from "../static/loading-black.svg";
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
  const randomAbort = useRef(new AbortController());
  const matchingAbort = useRef(new AbortController());
  const matchingDebounce = useRef<number>();

  const { setState } = props;

  /** Get a random page as input. */
  const fetchRandom = useCallback(() => {
    setState("loadingRandom");
    randomAbort.current.abort();
    const thisAbort = new AbortController();
    randomAbort.current = thisAbort;
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
          toast.error("An unexpected error occurred while getting random page");
          console.error(err);
        }
      });
  }, [setState, props.database]);

  /** Lazily fetch matching when needed. */
  useEffect(() => {
    if (props.state !== "loadingRandom" && props.state.matching === "loading") {
      matchingAbort.current.abort();
      const thisAbort = new AbortController();
      matchingAbort.current = thisAbort;

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

      matchingDebounce.current = setTimeout(() => {
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
          });
      }, SEARCH_DEBOUNCE_MS);
    }
  }, [props.state, props.database, setState]);

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
        <div className="z-50 absolute left-2 right-2 shadow-2xl">
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
                  className="m-1 p-0"
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
        disabled={props.disabled || props.state === "loadingRandom"}
        className="w-6 h-6 absolute right-4 top-0 bottom-0 my-auto opacity-30 hover:opacity-80"
        onClick={fetchRandom}
      />
    </div>
  );
};
