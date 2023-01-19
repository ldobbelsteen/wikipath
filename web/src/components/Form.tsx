import React, { useEffect, useState } from "react";
import { Page } from "../api";
import Swap from "../static/swap.svg";
import { Language } from "./Language";
import { Search } from "./Search";

export const Form = (props: {
  disabled: boolean;
  submit: (langCode: string, source: number, target: number) => void;
}) => {
  const [langCode, setlangCode] = useState<string>();

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
  }, [langCode]);

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
      if (langCode && sourcePage && targetPage) {
        setSourceInput(sourcePage.title);
        setTargetInput(targetPage.title);
        props.submit(langCode, sourcePage.id, targetPage.id);
      }
    }
  }

  /** If both inputs are ready and we're waiting, find */
  useEffect(() => {
    if (sourceReady && targetReady && waitingForReady) find();
  });

  return (
    <div>
      <p className="m-0">
        Find the shortest path between any two Wikipedia pages
      </p>
      <div className="flex justify-center items-center flex-wrap">
        <Language
          disabled={props.disabled}
          selectedlangCode={langCode}
          setSelectedlangCode={setlangCode}
        />
        <Search
          input={sourceInput}
          invalid={sourceInvalid}
          langCode={langCode}
          disabled={props.disabled}
          placeholder={"Starting page"}
          setReady={setSourceReady}
          setInput={setSourceInput}
          setPage={setSourcePage}
        />
        <input
          className="w-8 h-8 m-0 p-0 bg-white/0"
          type="image"
          src={Swap}
          alt="Get random page"
          disabled={props.disabled}
          onClick={swap}
        />
        <Search
          input={targetInput}
          invalid={targetInvalid}
          langCode={langCode}
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
