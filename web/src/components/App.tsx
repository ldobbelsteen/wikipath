import "../styles.scss";
import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import { Toaster, toast } from "react-hot-toast";
import { Graph, HTTP } from "../api";
import { PageInputForm } from "./PageInputForm";
import { ResultGraph } from "./ResultGraph";

const App = () => {
  const [graph, setGraph] = useState<Graph | string | undefined>("");

  const submitForm = (
    languageCode: string,
    sourceId: number,
    targetId: number
  ) => {
    setGraph(undefined);
    HTTP.getGraph(languageCode, sourceId, targetId)
      .then(setGraph)
      .catch((err) => {
        toast.error("An unexpected error occurred while getting your graph :(");
        console.error(err);
      });
  };

  return (
    <>
      <header>
        <a href="/">Wikipath</a>
      </header>
      <PageInputForm disabled={graph === undefined} submitForm={submitForm} />
      <ResultGraph isLoading={graph === undefined} graph={graph} />
      <div className="bottom-right">
        <a href="https://github.com/ldobbelsteen/wikipath">Source code</a>
      </div>
      <Toaster />
    </>
  );
};

const container = document.getElementById("root");
if (container) {
  const root = createRoot(container);
  root.render(<App />);
}
