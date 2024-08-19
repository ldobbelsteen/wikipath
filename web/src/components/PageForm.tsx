import React, { useCallback, useEffect, useState } from "react";
import toast from "react-hot-toast";
import LoadingBlack from "../static/loading-black.svg";
import LoadingWhite from "../static/loading-white.svg";
import Swap from "../static/swap.svg";
import { SearchState, PageSearch, defaultSearchState } from "./PageSearch";
import { Button } from "./generic/Button";
import { InputImage } from "./generic/InputImage";
import { Database, Page } from "../schema";

export const PageForm = (props: {
  database: Database;
  pathsLoading: boolean;
  submit: (database: Database, source: Page, target: Page) => void;
}) => {
  const [pendingFind, setPendingFind] = useState(false);
  const [pendingSwap, setPendingSwap] = useState(false);
  const [source, setSource] = useState<SearchState | "loadingRandom">(
    defaultSearchState(),
  );
  const [target, setTarget] = useState<SearchState | "loadingRandom">(
    defaultSearchState(),
  );

  const { submit } = props;

  /** Request the shortest paths with the current form input. If anything is
   * still loading, we set it to be pending. */
  const find = useCallback(() => {
    if (
      source !== "loadingRandom" &&
      target !== "loadingRandom" &&
      source.matching !== "loading" &&
      target.matching !== "loading"
    ) {
      setPendingFind(false);
      if (source.matching.page === undefined) {
        setSource({ ...source, showUnknown: true });
        toast.error("Start page is not known");
        return;
      }
      if (target.matching.page === undefined) {
        setTarget({ ...target, showUnknown: true });
        toast.error("End page is not known");
        return;
      }
      submit(props.database, source.matching.page, target.matching.page);
    } else {
      setPendingFind(true);
    }
  }, [props.database, source, target, submit]);

  /** Swap the two inputs. */
  const swap = useCallback(() => {
    if (
      source !== "loadingRandom" &&
      target !== "loadingRandom" &&
      source.matching !== "loading" &&
      target.matching !== "loading"
    ) {
      setPendingSwap(false);
      const tmp = source;
      setSource(target);
      setTarget(tmp);
    } else {
      setPendingSwap(true);
    }
  }, [source, target]);

  /** Handle pending find when loading is finished. Swapping has precedence. */
  useEffect(() => {
    if (pendingFind && !pendingSwap) {
      find();
    }
  }, [find, pendingFind, pendingSwap]);

  /** Handle pending swap when loading is finished. */
  useEffect(() => {
    if (pendingSwap) {
      swap();
    }
  }, [pendingSwap, swap]);

  /** Clear input on database change. */
  useEffect(() => {
    setSource(defaultSearchState());
    setTarget(defaultSearchState());
  }, [props.database]);

  return (
    <>
      <PageSearch
        state={source}
        setState={setSource}
        database={props.database}
        disabled={props.pathsLoading}
        placeholder="Start page"
      />
      <InputImage
        alt="Swap pages"
        src={pendingSwap ? LoadingWhite : Swap}
        disabled={props.pathsLoading || pendingSwap || pendingFind}
        className="hover:brightness-75"
        onClick={swap}
      />
      <PageSearch
        state={target}
        setState={setTarget}
        database={props.database}
        disabled={props.pathsLoading}
        placeholder="End page"
      />
      <Button disabled={props.pathsLoading || pendingFind} onClick={find}>
        <div className="size-6">
          {pendingFind || props.pathsLoading ? (
            <img src={LoadingBlack} alt="Loading..." />
          ) : (
            <span>Go</span>
          )}
        </div>
      </Button>
    </>
  );
};
