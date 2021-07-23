import React, { useEffect, useState } from "react";
import FindButton from "./FindButton";
import LanguageSelect from "./LanguageSelect";
import { Page } from "../helpers/api";
import SearchInput from "./SearchInput";
import Swap from "../static/swap.svg";

export default function PageForm(props: {
  isLoading: boolean;
  fetchGraph: (source: Page, target: Page) => void;
}): JSX.Element {
  const [languageCode, setLanguageCode] = useState("");
  const [sourceInput, setSourceInput] = useState("");
  const [targetInput, setTargetInput] = useState("");
  const [sourcePage, setSourcePage] = useState<Page>();
  const [targetPage, setTargetPage] = useState<Page>();
  const [sourceInvalid, setSourceInvalid] = useState(false);
  const [targetInvalid, setTargetInvalid] = useState(false);

  const [sourceReady, setSourceReady] = useState(true);
  const [targetReady, setTargetReady] = useState(true);
  const [waitingForReady, setWaitingForReady] = useState(false);

  // Clear input on language change
  useEffect(() => {
    setSourcePage(undefined);
    setSourceInput("");
    setTargetPage(undefined);
    setTargetInput("");
  }, [languageCode]);

  // Remove invalid error on input change
  useEffect(() => {
    setSourceInvalid(false);
  }, [sourceInput]);
  useEffect(() => {
    setTargetInvalid(false);
  }, [targetInput]);

  // Swap source and target inputs
  function swap() {
    const temp = sourcePage;
    const tempInput = sourceInput;
    setSourcePage(targetPage);
    setSourceInput(targetInput);
    setTargetPage(temp);
    setTargetInput(tempInput);
  }

  // Fetch the shortest path(s)
  function find() {
    const ready = sourceReady && targetReady;
    setWaitingForReady(!ready);
    if (ready) {
      setSourceInvalid(!sourcePage);
      setTargetInvalid(!targetPage);
      if (sourcePage && targetPage) {
        setSourceInput(sourcePage.title);
        setTargetInput(targetPage.title);
        props.fetchGraph(sourcePage, targetPage);
      }
    }
  }

  // If both inputs are ready and we're waiting, find
  useEffect(() => {
    if (sourceReady && targetReady && waitingForReady) find();
  });

  return (
    <div className="form">
      <p>Find the shortest path between any two Wikipedia pages</p>
      <div>
        <LanguageSelect disabled={props.isLoading} selected={setLanguageCode} />
        <SearchInput
          id={"source"}
          input={sourceInput}
          invalid={sourceInvalid}
          languageCode={languageCode}
          disabled={props.isLoading}
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
          disabled={props.isLoading}
          onClick={swap}
        ></input>
        <SearchInput
          id={"target"}
          input={targetInput}
          invalid={targetInvalid}
          languageCode={languageCode}
          disabled={props.isLoading}
          placeholder={"End page"}
          setReady={setTargetReady}
          setInput={setTargetInput}
          setPage={setTargetPage}
        />
        <FindButton
          disabled={props.isLoading || waitingForReady}
          text="Find!"
          onClick={find}
        />
      </div>
    </div>
  );
}
