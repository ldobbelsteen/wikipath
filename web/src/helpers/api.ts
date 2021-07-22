export type Database = {
  dumpDate: string;
  languageName: string;
  languageCode: string;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isDatabase(database: any): database is Database {
  return (
    database.languageCode &&
    typeof database.languageCode === "string" &&
    database.dumpDate &&
    typeof database.dumpDate === "string" &&
    database.languageName &&
    typeof database.languageName === "string"
  );
}

export type Page = {
  id: number;
  title: string;
  languageCode: string;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isPage(page: any): page is Page {
  return (
    page.id &&
    Number.isInteger(page.id) &&
    page.id > 0 &&
    page.title &&
    typeof page.title === "string" &&
    page.languageCode &&
    typeof page.languageCode === "string"
  );
}

export type Graph = {
  pageNames: Record<number, string>;
  outgoingLinks: Record<number, number[]>;
  pathCount: number;
  pathDegree: number;
  sourcePage: number;
  targetPage: number;
  sourceIsRedir: boolean;
  targetIsRedir: boolean;
  languageCode: string;
  searchDuration: number;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isGraph(graph: any): graph is Graph {
  if (
    !graph.pageNames ||
    typeof graph.pageNames !== "object" ||
    graph.pageNames === null
  )
    return false;
  Object.values(graph.pageNames).forEach((val) => {
    if (typeof val !== "string") return false;
  });
  if (
    !graph.outgoingLinks ||
    typeof graph.outgoingLinks !== "object" ||
    graph.outgoingLinks === null
  )
    return false;
  Object.values(graph.outgoingLinks).forEach((arr) => {
    if (!Array.isArray(arr)) return false;
    if (arr.some((val) => typeof val !== "number")) return false;
  });
  return (
    Number.isInteger(graph.pathCount) &&
    Number.isInteger(graph.pathDegree) &&
    Number.isInteger(graph.sourcePage) &&
    Number.isInteger(graph.targetPage) &&
    typeof graph.sourceIsRedir === "boolean" &&
    typeof graph.targetIsRedir === "boolean" &&
    graph.languageCode &&
    typeof graph.languageCode === "string" &&
    graph.searchDuration &&
    typeof graph.searchDuration === "number"
  );
}

// Fetch the array of available databases
export async function getAvailableDatabases(): Promise<Database[]> {
  const url = "/databases";
  const res = await fetch(url);
  const data = await res.json();
  if (!Array.isArray(data)) {
    return Promise.reject("Databases response is not an array");
  }
  if (!data.every(isDatabase)) {
    return Promise.reject("Unexpected database format");
  }
  if (data.length < 1) {
    return Promise.reject("Zero available databases");
  }
  return data;
}

// Fetch a random page in a certain language
export async function getRandomPage(languageCode: string): Promise<Page> {
  const url = `/random?language=${languageCode}`;
  const res = await fetch(url);
  const data = await res.json();
  data.languageCode = languageCode;
  if (!isPage(data)) {
    return Promise.reject("Unexpected page format");
  }
  return data;
}

// Fetch the shortest paths between two pages
export async function getShortestPaths(
  source: Page,
  target: Page
): Promise<Graph> {
  const start = new Date();
  if (source.languageCode !== target.languageCode) {
    return Promise.reject("Source and target aren't in the same language");
  }
  const url = `/paths?language=${source.languageCode}&source=${source.id}&target=${target.id}`;
  const res = await fetch(url);
  const data = await res.json();
  data.searchDuration = new Date().getTime() - start.getTime();
  if (!isGraph(data)) {
    return Promise.reject("Unexpected graph format");
  }
  return data;
}

// Get search suggestions from the Wikipedia API in a certain language
export async function getSuggestions(
  search: string,
  languageCode: string,
  limit: number,
  abort: AbortSignal
): Promise<Page[]> {
  const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&list=prefixsearch&pslimit=${limit}&pssearch=${search}&format=json`;
  const headers = {
    "Api-User-Agent":
      "Wikipath/1.0 (https://github.com/ldobbelsteen/wikipath/)",
  };
  const res = await fetch(url, {
    headers: headers,
    signal: abort,
  });
  const data = await res.json();
  const results = data?.query?.prefixsearch;
  if (!results) {
    return Promise.reject("Empty suggestions response");
  }
  if (!Array.isArray(results)) {
    return Promise.reject("Suggestions response is not an array");
  }
  const suggestions = results.map((res) => {
    return { title: res.title, id: res.pageid, languageCode: languageCode };
  });
  if (!suggestions.every(isPage)) {
    return Promise.reject("Unexpected search suggestion format");
  }
  return suggestions;
}
