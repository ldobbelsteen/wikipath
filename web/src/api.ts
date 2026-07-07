import { z } from "zod";
import { flattenUnique } from "./misc";
import {
  type Database,
  DatabaseSchema,
  type Page,
  type Paths,
  PathsSchema,
  WikipediaRandomSchema,
  WikipediaSearchSchema,
  WikipediaTitlesSchema,
} from "./schema";
import { getTitle, storeTitle } from "./storage";

// Browser JS can't set the User-Agent header, so the User-Agent Policy recommends the
// `Api-User-Agent` header instead to identify the bot.
// https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy#Code_examples
const headers = {
  "Api-User-Agent": `Wikipath/${import.meta.env.VERSION} (https://github.com/ldobbelsteen/wikipath/)`,
};

const get = async <T, U>(
  url: string,
  schema: z.Schema<T, U>,
  abort?: AbortSignal,
): Promise<T> => {
  const attempt = async (): Promise<Response> => {
    const res = await fetch(url, {
      signal: abort,
      headers: headers,
      method: "GET",
    });
    if (res.status === 429) {
      // Robot policy "Generally applicable rules", rule #6 "Respect our HTTP status codes":
      // honor the Retry-After delay on 429 responses.
      // https://wikitech.wikimedia.org/wiki/Robot_policy#Generally_applicable_rules
      const retryAfter = res.headers.get("Retry-After");
      const delayMs = retryAfter
        ? Number.parseInt(retryAfter, 10) * 1000
        : 5000;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      return fetch(url, { signal: abort, headers: headers, method: "GET" });
    }
    return res;
  };

  const res = await attempt();
  if (res.ok) {
    const parse = await schema.safeParseAsync(await res.json());
    if (!parse.success) {
      return Promise.reject(parse.error);
    }
    return parse.data;
  }
  return Promise.reject(new Error(await res.text()));
};

export const listDatabases = async (): Promise<Database[]> => {
  const url = "/api/list_databases";
  const result = await get(url, z.array(DatabaseSchema));
  return result;
};

export const fetchShortestPaths = async (
  database: Database,
  sourceId: number,
  targetId: number,
  maxPaths = 8,
): Promise<Paths> => {
  const url = `/api/shortest_paths?language-code=${database.languageCode}&date-code=${database.dateCode}&source=${sourceId.toString()}&target=${targetId.toString()}`;
  const result = await get(url, PathsSchema);
  const pathsOfIds = extractFullPaths(result.source, result.links, maxPaths);
  const titles = await fetchTitles(
    result.languageCode,
    flattenUnique(pathsOfIds),
  );
  const idToPage = (id: number): Page => ({ id, title: titles[id] });
  return {
    ...result,
    source: idToPage(result.source),
    target: idToPage(result.target),
    paths: pathsOfIds.map((path) => path.map(idToPage)),
  };
};

export const fetchRandomPage = async (languageCode: string): Promise<Page> => {
  const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&list=random&rnnamespace=0&rnlimit=1`;
  const result = await get(url, WikipediaRandomSchema);
  storeTitle(result.id.toString(), result.title);
  return result;
};

const fetchTitles = async (
  languageCode: string,
  pageIds: number[],
): Promise<Record<number, string>> => {
  const result: Record<number, string> = {};

  const unknownLocally = pageIds.filter((pageId) => {
    const cached = getTitle(pageId.toString());
    if (cached) {
      result[pageId] = cached;
      return false;
    }
    return true;
  });

  async function fetchTitles(ids: number[]) {
    if (ids.length === 0) return;
    const limit = 50;
    if (ids.length > limit) {
      const right = ids.slice(limit);
      const left = ids.slice(0, limit);
      await fetchTitles(right);
      await fetchTitles(left);
    } else {
      const delimited = ids.join("|");
      const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&pageids=${delimited}`;
      const titles = await get(url, WikipediaTitlesSchema);
      for (const [id, title] of Object.entries(titles)) {
        storeTitle(id, title);
        result[Number.parseInt(id, 10)] = title;
      }
    }
  }

  await fetchTitles(unknownLocally);
  return result;
};

export const fetchSuggestions = async (
  languageCode: string,
  searchString: string,
  resultLimit: number,
  abort: AbortSignal,
): Promise<Page[]> => {
  const url = `https://${languageCode}.wikipedia.org/w/rest.php/v1/search/title?q=${searchString}&limit=${resultLimit.toString()}`;
  const result = await get(url, WikipediaSearchSchema, abort);
  for (const page of result) {
    storeTitle(page.id.toString(), page.title);
  }
  return result;
};

function extractFullPaths(
  source: number,
  links: Record<number, number[]>,
  maxPaths: number,
): number[][] {
  const result: number[][] = [];
  const recurse = (current: number, currentPath: number[]): boolean => {
    if (current in links && links[current].length > 0) {
      const targets = links[current];
      targets.sort();
      for (const target of targets) {
        const maxReached = recurse(target, [...currentPath, target]);
        if (maxReached) {
          return true;
        }
      }
    } else {
      result.push(currentPath);
      if (result.length >= maxPaths) {
        return true;
      }
    }
    return false;
  };
  recurse(source, [source]);
  return result;
}
