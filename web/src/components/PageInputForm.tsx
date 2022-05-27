import React, { useEffect, useState } from "react";
import { Page } from "../api";
import Swap from "../static/swap.svg";
import { PageInputLanguage } from "./PageInputLanguage";
import { PageInputSearch } from "./PageInputSearch";

export const PageInputForm = (props: {
  disabled: boolean;
  submitForm: (
    languageCode: string,
    sourceId: number,
    targetId: number
  ) => void;
}) => {
  const [languageCode, setLanguageCode] = useState<string>();

  const [sourceInput, setSourceInput] = useState("");
  const [targetInput, setTargetInput] = useState("");
  const [sourcePage, setSourcePage] = useState<Page>();
  const [targetPage, setTargetPage] = useState<Page>();
  const [sourceInvalid, setSourceInvalid] = useState(false);
  const [targetInvalid, setTargetInvalid] = useState(false);

  const [sourceReady, setSourceReady] = useState(true);
  const [targetReady, setTargetReady] = useState(true);
  const [waitingForReady, setWaitingForReady] = useState(false);

  /** Clear input on language change */
  useEffect(() => {
    setSourcePage(undefined);
    setSourceInput("");
    setTargetPage(undefined);
    setTargetInput("");
  }, [languageCode]);

  /** Remove invalid error on input change */
  useEffect(() => {
    setSourceInvalid(false);
  }, [sourceInput]);
  useEffect(() => {
    setTargetInvalid(false);
  }, [targetInput]);

  /** Swap source and target inputs */
  function swap() {
    const temp = sourcePage;
    const tempInput = sourceInput;
    setSourcePage(targetPage);
    setSourceInput(targetInput);
    setTargetPage(temp);
    setTargetInput(tempInput);
  }

  /** Fetch the shortest path(s) */
  function find() {
    const ready = sourceReady && targetReady;
    setWaitingForReady(!ready);
    if (ready) {
      setSourceInvalid(!sourcePage);
      setTargetInvalid(!targetPage);
      if (languageCode && sourcePage && targetPage) {
        setSourceInput(sourcePage.title);
        setTargetInput(targetPage.title);
        props.submitForm(languageCode, sourcePage.id, targetPage.id);
      }
    }
  }

  /** If both inputs are ready and we're waiting, find */
  useEffect(() => {
    if (sourceReady && targetReady && waitingForReady) find();
  });

  return (
    <div className="form">
      <p>Find the shortest path between any two Wikipedia pages</p>
      <div id="form-div">
        <PageInputLanguage
          disabled={props.disabled}
          selectedLanguageCode={languageCode}
          setSelectedLanguageCode={setLanguageCode}
        />
        <PageInputSearch
          id={"source"}
          input={sourceInput}
          invalid={sourceInvalid}
          languageCode={languageCode}
          disabled={props.disabled}
          placeholder={"Starting page"}
          setReady={setSourceReady}
          setInput={setSourceInput}
          setPage={setSourcePage}
        />
        <input
          className="swap"
          type="image"
          src={Swap}
          alt="Get random page"
          disabled={props.disabled}
          onClick={swap}
        ></input>
        <PageInputSearch
          id={"target"}
          input={targetInput}
          invalid={targetInvalid}
          languageCode={languageCode}
          disabled={props.disabled}
          placeholder={"End page"}
          setReady={setTargetReady}
          setInput={setTargetInput}
          setPage={setTargetPage}
        />
        <button disabled={props.disabled || waitingForReady} onClick={find}>
          Find!
        </button>
      </div>
    </div>
  );
};
