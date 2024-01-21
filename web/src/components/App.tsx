import React, { useState } from "react";
import { createRoot } from "react-dom/client";
import { Toaster, toast } from "react-hot-toast";
import { Paths, Api, Database, Page } from "../api";
import { DatabaseSelect } from "./DatabaseSelect";
import { PageForm } from "./PageForm";
import { PathsGraph } from "./PathsGraph";
import { Link } from "./generic/Link";

const App = () => {
  const [database, setDatabase] = useState<Database>();
  const [paths, setPaths] = useState<Paths | "loading">();

  const getPaths = (database: Database, source: Page, target: Page) => {
    setPaths("loading");
    Api.shortestPaths(database, source.id, target.id)
      .then(setPaths)
      .catch((err) => {
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
      <section className="flex flex-wrap justify-center items-center">
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
      <main className="grow">
        <PathsGraph paths={paths} />
      </main>
      <Toaster />
    </>
  );
};

const container = document.getElementById("root");
if (container) {
  const root = createRoot(container);
  root.render(<App />);
}
