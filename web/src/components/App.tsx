import "../index.css";
import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import { Toaster, toast } from "react-hot-toast";
import { Paths, HTTP } from "../api";
import { Form } from "./Form";
import { Graph } from "./Graph";

const App = () => {
  const [paths, setPaths] = useState<Paths | string | undefined>("");

  const submitForm = (
    languageCode: string,
    sourceId: number,
    targetId: number,
  ) => {
    setPaths(undefined);
    HTTP.shortestPaths(languageCode, sourceId, targetId)
      .then(setPaths)
      .catch((err) => {
        toast.error("An unexpected error occurred while getting your graph :(");
        console.error(err);
      });
  };

  return (
    <>
      <header>
        <h1 className="text-4xl font-bold mt-2">
          <a href="/">Wikipath</a>
        </h1>
      </header>
      <Form disabled={paths === undefined} submit={submitForm} />
      <Graph isLoading={paths === undefined} paths={paths} />
      <div className="absolute bottom-0 right-0 m-1">
        <a href="https://github.com/ldobbelsteen/wikipath">Source code</a>
      </div>
      <Toaster />
    </>
  );
};

const container = document.getElementsByTagName("main").item(0);
if (container) {
  const root = createRoot(container);
  root.render(<App />);
}
