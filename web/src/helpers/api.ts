import { flattenUnique, pseudoRandomShuffle } from "../helpers/misc";

const developmentPrefix = ""; // e.g. http://localhost:1789
const wikipediaHeaders = {
  "Api-User-Agent": "Wikipath/1.0 (https://github.com/ldobbelsteen/wikipath/)",
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isNonEmptyString(str: any): str is string {
  return typeof str === "string" && str.length > 0;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isNonZeroPositiveInt(int: any): int is number {
  return Number.isInteger(int) && int > 0;
}

export type Database = {
  dumpDate: string;
  languageName: string;
  languageCode: string;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isDatabase(database: any): database is Database {
  return (
    isNonEmptyString(database.dumpDate) &&
    isNonEmptyString(database.languageName) &&
    isNonEmptyString(database.languageCode)
  );
}

export type Page = {
  id: number;
  title: string;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isPage(page: any): page is Page {
  return page && isNonZeroPositiveInt(page.id) && isNonEmptyString(page.title);
}

type RawGraph = {
  links: Record<number, number[]>;
  count: number;
  degree: number;
  source: number;
  target: number;
  sourceRedir: boolean;
  targetRedir: boolean;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isRawGraph(graph: any): graph is RawGraph {
  if (!graph) return false;
  if (typeof graph.links !== "object" || graph.links === null) return false;
  for (const [page, outgoing] of Object.entries(graph.links)) {
    const pageID = parseInt(page);
    if (!isNonZeroPositiveInt(pageID)) return false;
    if (!Array.isArray(outgoing)) return false;
    if (!outgoing.every(isNonZeroPositiveInt)) return false;
  }
  return (
    Number.isInteger(graph.count) &&
    Number.isInteger(graph.degree) &&
    isNonZeroPositiveInt(graph.source) &&
    isNonZeroPositiveInt(graph.target) &&
    typeof graph.sourceRedir === "boolean" &&
    typeof graph.targetRedir === "boolean"
  );
}

// Extract a maximum of n paths from a raw graph, pseudo-randomly
function extractPaths(graph: RawGraph, maxPaths: number): number[][] {
  const result: number[][] = [];
  const recurse = (page: number, path: number[]): boolean => {
    let outgoing = graph.links[page];
    if (outgoing && outgoing.length > 0) {
      outgoing = pseudoRandomShuffle(outgoing);
      for (let i = 0; i < outgoing.length; i++) {
        const maxReached = recurse(outgoing[i], [...path, outgoing[i]]);
        if (maxReached) {
          return true;
        }
      }
    } else {
      result.push(path);
      if (result.length >= maxPaths) return true;
    }
    return false;
  };
  recurse(graph.source, [graph.source]);
  return result;
}

export type Graph = {
  paths: Page[][];
  count: number;
  degree: number;
  source: number;
  target: number;
  sourceRedir: boolean;
  targetRedir: boolean;
  requestTime: Date;
  languageCode: string;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isGraph(graph: any): graph is Graph {
  if (!graph) return false;
  const paths = graph.paths;
  if (!Array.isArray(paths)) return false;
  if (
    !paths.every((path) => {
      if (!Array.isArray(path)) return false;
      return path.every(isPage);
    })
  )
    return false;
  return (
    Number.isInteger(graph.count) &&
    Number.isInteger(graph.degree) &&
    isNonZeroPositiveInt(graph.source) &&
    isNonZeroPositiveInt(graph.target) &&
    typeof graph.sourceRedir === "boolean" &&
    typeof graph.targetRedir === "boolean" &&
    graph.requestTime instanceof Date &&
    isNonEmptyString(graph.languageCode)
  );
}

// Fetch the array of available databases from the Wikipath API
export async function getAvailableDatabases(): Promise<Database[]> {
  const url = developmentPrefix + "/databases";
  const res = await fetch(url);
  const data = await res.json();
  if (!Array.isArray(data)) {
    return Promise.reject("Databases response is malformed");
  }
  if (!data.every(isDatabase)) {
    return Promise.reject("Unexpected database format");
  }
  return data;
}

// Fetch a random page from the Wikipedia API
export async function getRandomPage(languageCode: string): Promise<Page> {
  const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&list=random&rnnamespace=0&rnlimit=1`;
  const res = await fetch(url, { headers: wikipediaHeaders });
  const raw = await res.json();
  const result = raw?.query?.random;
  if (!Array.isArray(result) || result.length !== 1) {
    return Promise.reject("Random page response is malformed");
  }
  const page = {
    id: result[0]["id"],
    title: result[0]["title"],
  };
  if (!isPage(page)) {
    return Promise.reject("Unexpected random page format");
  }
  return page;
}

// Fetch the shortest paths between two pages from the Wikipath API
// Uses the Wikipedia API to convert page ID's to their titles.
export async function getShortestPaths(
  source: Page,
  target: Page,
  languageCode: string,
  maxPaths: number
): Promise<Graph> {
  const start = new Date();
  const url =
    developmentPrefix +
    `/paths?language=${languageCode}&source=${source.id}&target=${target.id}`;
  const res = await fetch(url);
  const rawGraph = await res.json();
  if (!isRawGraph(rawGraph)) {
    return Promise.reject("Unexpected shortest paths format");
  }

  // Extract paths from the raw API graph and fetch titles
  const paths = extractPaths(rawGraph, maxPaths);
  const ids = flattenUnique(paths);
  const titles = await getPageTitles(ids, languageCode);
  const titleMapper: Record<number, string> = {};
  for (let i = 0; i < ids.length; i++) {
    titleMapper[ids[i]] = titles[i];
  }

  // Convert the raw graph to a more usable graph
  const graph = rawGraph as unknown as Graph;
  graph.languageCode = languageCode;
  graph.requestTime = start;
  graph.paths = paths.map((path) =>
    path.map((page) => {
      return {
        id: page,
        title: titleMapper[page],
      };
    })
  );

  // Verify graph structure
  if (!isGraph(graph)) {
    return Promise.reject("Unexpected graph format");
  }

  return graph;
}

// Fetch the set of titles from the Wikipedia API
export async function getPageTitles(
  pages: number[],
  languageCode: string
): Promise<string[]> {
  if (pages.length > 50) {
    const left = pages.slice(0, 50);
    const right = pages.slice(50);
    const leftTitles = await getPageTitles(left, languageCode);
    const rightTitles = await getPageTitles(right, languageCode);
    return leftTitles.concat(rightTitles);
  }
  const delimitedPages = pages.join("|");
  const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&pageids=${delimitedPages}`;
  const res = await fetch(url, { headers: wikipediaHeaders });
  const data = await res.json();
  const titles = pages.map((id) => data?.query?.pages[id]?.title);
  if (!titles.every(isNonEmptyString)) {
    return Promise.reject("Page's title could not be determined");
  }
  return titles;
}

// Get search suggestions from the Wikipedia API
export async function getSuggestions(
  search: string,
  languageCode: string,
  limit: number,
  abort: AbortSignal
): Promise<Page[]> {
  const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&list=prefixsearch&pslimit=${limit}&pssearch=${search}&format=json`;
  const res = await fetch(url, { headers: wikipediaHeaders, signal: abort });
  const data = await res.json();
  const results = data?.query?.prefixsearch;
  if (!results || !Array.isArray(results)) {
    return Promise.reject("Suggestions response is malformed");
  }
  const suggestions = results.map((res) => {
    return { title: res.title, id: res.pageid };
  });
  if (!suggestions.every(isPage)) {
    return Promise.reject("Unexpected suggestions format");
  }
  return suggestions;
}
