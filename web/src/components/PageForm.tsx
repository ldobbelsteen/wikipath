import React, { useEffect, useState } from "react";
import toast from "react-hot-toast";
import { Database, Page } from "../api";
import LoadingBlack from "../assets/loading-black.svg";
import LoadingWhite from "../assets/loading-white.svg";
import Swap from "../assets/swap.svg";
import { SearchState, PageSearch, defaultSearchState } from "./PageSearch";
import { Button } from "./generic/Button";
import { InputImage } from "./generic/InputImage";

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

  /** Clear input on language change. */
  useEffect(() => {
    setSource(defaultSearchState());
    setTarget(defaultSearchState());
  }, [props.database]);

  /** If there is a pending find command, execute it once everything is loaded. */
  useEffect(() => {
    if (
      pendingFind &&
      !pendingSwap &&
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
    }
  }, [pendingSwap, pendingFind, source, target, submit, props.database]);

  /** If there is a pending swap command, execute it once everything is loaded. */
  useEffect(() => {
    if (
      pendingSwap &&
      source !== "loadingRandom" &&
      target !== "loadingRandom" &&
      source.matching !== "loading" &&
      target.matching !== "loading"
    ) {
      setPendingSwap(false);
      const tmp = source;
      setSource(target);
      setTarget(tmp);
    }
  }, [pendingSwap, source, target]);

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
        src={pendingSwap ? LoadingWhite : Swap}
        alt="Swap pages"
        className="w-8 h-8"
        disabled={props.pathsLoading || pendingSwap}
        onClick={() => setPendingSwap(true)}
      />
      <PageSearch
        state={target}
        setState={setTarget}
        database={props.database}
        disabled={props.pathsLoading}
        placeholder="End page"
      />
      <Button
        disabled={props.pathsLoading || pendingFind}
        onClick={() => setPendingFind(true)}
      >
        {pendingFind || props.pathsLoading ? (
          <img className="w-6 h-6" src={LoadingBlack} alt="Loading..." />
        ) : (
          "Find!"
        )}
      </Button>
    </>
  );
};
