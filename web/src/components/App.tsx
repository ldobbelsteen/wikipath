import "../styles.scss";
import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import { Graph, HTTP } from "../api";
import { PageInputForm } from "./PageInputForm";
import { ResultGraph } from "./ResultGraph";

const App = () => {
  const [isLoading, setLoading] = useState(false);
  const [graph, setGraph] = useState<Graph | string>();

  const getGraph = (
    languageCode: string,
    sourceId: number,
    targetId: number
  ) => {
    setLoading(true);
    HTTP.getGraph(languageCode, sourceId, targetId)
      .then(setGraph)
      .finally(() => setLoading(false))
      .catch(console.error);
  };

  return (
    <>
      <header>
        <a href="/">Wikipath</a>
      </header>
      <PageInputForm isLoading={isLoading} getGraph={getGraph} />
      <ResultGraph isLoading={isLoading} graph={graph} />
      <div className="bottom-right">
        <a href="https://github.com/ldobbelsteen/wikipath">Source code</a>
      </div>
    </>
  );
};

const container = document.getElementById("root");
if (container) {
  const root = createRoot(container);
  root.render(<App />);
}
