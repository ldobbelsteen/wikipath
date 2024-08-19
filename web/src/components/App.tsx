import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import { Toaster, toast } from "react-hot-toast";
import { DatabaseSelect } from "./DatabaseSelect";
import { PageForm } from "./PageForm";
import { PathsGraph } from "./PathsGraph";
import { Link } from "./generic/Link";
import { Database, Page, Paths } from "../schema";
import { fetchShortestPaths } from "../api";

const App = () => {
  const [database, setDatabase] = useState<Database>();
  const [paths, setPaths] = useState<Paths | "loading">();

  const getPaths = (database: Database, source: Page, target: Page) => {
    setPaths("loading");
    fetchShortestPaths(database, source.id, target.id)
      .then(setPaths)
      .catch((err: unknown) => {
        setPaths(undefined);
        toast.error("An unexpected error occurred while getting your paths");
        console.error(err);
      });
  };

  return (
    <>
      <header>
        <h1 className="text-4xl font-bold">
          <a href="/">Wikipath</a>
        </h1>
        <span>
          Find the shortest path between any two Wikipedia pages (
          <Link href="https://github.com/ldobbelsteen/wikipath">
            source code
          </Link>
          ).
        </span>
      </header>
      <section className="flex flex-wrap items-center justify-center">
        <DatabaseSelect
          selected={database}
          setSelected={setDatabase}
          disabled={paths === "loading"}
        />
        {database !== undefined && (
          <PageForm
            database={database}
            pathsLoading={paths === "loading"}
            submit={getPaths}
          />
        )}
      </section>
      <span>
        {paths === "loading" || paths === undefined
          ? "\u00A0"
          : paths.count === 0
            ? "No paths found"
            : `Found ${paths.count.toString()} ${
                paths.count === 1 ? "path" : "paths"
              } of degree ${paths.length.toString()}.${
                paths.count > paths.paths.length
                  ? ` A random sample of ${paths.paths.length.toString()} paths is shown below.`
                  : ""
              }`}
      </span>
      <PathsGraph className="grow" paths={paths} />
      <Toaster />
    </>
  );
};

const container = document.getElementById("root");
if (container) {
  const root = createRoot(container);
  root.render(<App />);
}
