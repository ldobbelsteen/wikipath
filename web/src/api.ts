import { z } from "zod";
import { flattenUnique } from "./misc";
import { getTitle, storeTitle } from "./storage";

export type Page = {
  id: number;
  title: string;
};

export type Database = {
  languageCode: string;
  dateCode: string;
};

export type Paths = {
  languageCode: string;
  dateCode: string;

  source: Page;
  sourceIsRedirect: boolean;
  target: Page;
  targetIsRedirect: boolean;

  paths: Page[][];
  length: number;
  count: number;
};

export abstract class Api {
  private static headers = {
    "Api-User-Agent":
      "Wikipath/1.1 (https://github.com/ldobbelsteen/wikipath/)",
  };

  private static get = async <T, U>(
    url: string,
    schema: z.Schema<T, z.ZodTypeDef, U>,
    abort?: AbortSignal,
  ): Promise<T> => {
    const res = await fetch(url, {
      signal: abort,
      headers: this.headers,
      method: "GET",
    });
    if (res.ok) {
      const parse = await schema.safeParseAsync(await res.json());
      if (!parse.success) {
        return Promise.reject(parse.error);
      } else {
        return parse.data;
      }
    } else {
      return Promise.reject(await res.text());
    }
  };

  static listDatabases = async (): Promise<Database[]> => {
    const url = "/api/list_databases";
    const result = await this.get(url, z.array(Schema.Database));
    return result;
  };

  static shortestPaths = async (
    database: Database,
    sourceId: number,
    targetId: number,
  ): Promise<Paths> => {
    const url = `/api/shortest_paths?language-code=${database.languageCode}&date-code=${database.dateCode}&source=${sourceId}&target=${targetId}`;
    const result = await this.get(url, Schema.Paths);
    const pathsOfIds = extractFullPaths(result.source, result.links, 8);
    const titles = await Api.titles(
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

  static randomPage = async (languageCode: string): Promise<Page> => {
    const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&list=random&rnnamespace=0&rnlimit=1`;
    const result = await this.get(url, Schema.WikipediaRandom);
    storeTitle(result.id.toString(), result.title);
    return result;
  };

  static titles = async (
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
        const titles = await Api.get(url, Schema.WikipediaTitles);
        Object.entries(titles).forEach(([id, title]) => {
          storeTitle(id, title);
          result[parseInt(id)] = title;
        });
      }
    }

    await fetchTitles(unknownLocally);
    return result;
  };

  static suggestions = async (
    languageCode: string,
    searchString: string,
    resultLimit: number,
    abort: AbortSignal,
  ): Promise<Page[]> => {
    const url = `https://${languageCode}.wikipedia.org/w/rest.php/v1/search/title?q=${searchString}&limit=${resultLimit}`;
    const result = await this.get(url, Schema.WikipediaSearch, abort);
    for (const page of result) {
      storeTitle(page.id.toString(), page.title);
    }
    return result;
  };
}

export abstract class Schema {
  static Id = z.number().int().nonnegative();
  static Title = z.string().min(1);

  static Page = z.object({
    id: this.Id,
    title: this.Title,
  });

  static Database = z.object({
    languageCode: z.string().min(1),
    dateCode: z.string().min(1),
  });

  static Paths = z.object({
    source: this.Id,
    sourceIsRedirect: z.boolean(),
    target: this.Id,
    targetIsRedirect: z.boolean(),
    links: z.record(
      z
        .string()
        .min(1)
        .transform((s) => parseInt(s)),
      z.array(this.Id),
    ),
    languageCode: z.string().min(1),
    dateCode: z.string().min(1),
    length: z.number().int().nonnegative(),
    count: z.number().int().nonnegative(),
  });

  static WikipediaRandom = z
    .object({
      query: z.object({
        random: z.array(this.Page).length(1),
      }),
    })
    .transform((obj) => obj.query.random[0]);

  static WikipediaTitles = z
    .object({
      query: z.object({
        pages: z.record(
          z.string().min(1),
          z.object({
            pageid: this.Id,
            title: this.Title,
          }),
        ),
      }),
    })
    .transform((obj) =>
      Object.values(obj.query.pages).reduce(
        (record, page) => ({ ...record, [page.pageid]: page.title }),
        {} as Record<number, string>,
      ),
    );

  static WikipediaSearch = z
    .object({
      pages: z.array(
        z.object({
          id: this.Id,
          title: this.Title,
        }),
      ),
    })
    .transform((obj) => obj.pages);
}

function extractFullPaths(
  source: number,
  links: Record<number, number[]>,
  maxPaths: number,
): number[][] {
  const result: number[][] = [];
  const recurse = (current: number, currentPath: number[]): boolean => {
    const targets = links[current];
    if (targets && targets.length > 0) {
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
