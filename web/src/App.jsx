import React, { useState } from "react";
import ReactDOM from "react-dom";

import "./App.scss";
import Header from "./components/Header.jsx";
import Form from "./components/Form.jsx";
import Graph from "./components/Graph.jsx";
import { getShortestPaths, setUrlParameters } from "./api";

function App() {
  const [isBusy, setBusy] = useState(false);
  const [data, setData] = useState({
    paths: [],
    language: "",
    time: 0,
  });

  async function search(source, target, language) {
    if (!isBusy) {
      setBusy(true);
      setUrlParameters({
        source: source,
        target: target,
        language: language,
      });
      const startTime = new Date();
      const result = await getShortestPaths(source, target, language);
      setBusy(false);
      if (result === "source") return "source";
      if (result === "target") return "target";
      setData({
        paths: result,
        language: language,
        time: new Date() - startTime,
      });
    }
  }

  return (
    <>
      <Header text="Wikipath" />
      <Form isBusy={isBusy} search={search} />
      <Graph isBusy={isBusy} data={data} />
      <div className="source">
        <a href="https://github.com/ldobbelsteen/wikipath">Source code</a>
      </div>
    </>
  );
}

ReactDOM.render(<App />, document.getElementById("root"));
