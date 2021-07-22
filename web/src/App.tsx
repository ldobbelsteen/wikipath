import "./styles.scss";
import { Graph, Page, getShortestPaths } from "./helpers/api";
import React, { useState } from "react";
import PageForm from "./components/PageForm";
import PathsGraph from "./components/PathsGraph";
import ReactDOM from "react-dom";
import SourceCode from "./components/SourceCode";
import TextHeader from "./components/TextHeader";

function App() {
  const [isLoading, setLoading] = useState(false);
  const [graph, setGraph] = useState<Graph>();

  async function fetchGraph(source: Page, target: Page) {
    setLoading(true);
    setGraph(await getShortestPaths(source, target));
    setLoading(false);
  }

  return (
    <>
      <TextHeader text="Wikipath" />
      <PageForm isLoading={isLoading} fetchGraph={fetchGraph} />
      <PathsGraph isLoading={isLoading} graph={graph} maxPaths={8} />
      <SourceCode url="https://github.com/ldobbelsteen/wikipath" />
    </>
  );
}

ReactDOM.render(<App />, document.getElementById("root"));
