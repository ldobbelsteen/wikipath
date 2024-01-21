import { z } from "zod";
import { flattenUnique, pseudoRandomShuffle } from "./misc";

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

  static listDatabases = () => {
    const url = "/api/list_databases";
    return this.get(url, z.array(Schema.Database));
  };

  static shortestPaths = (
    database: Database,
    sourceId: number,
    targetId: number,
  ) => {
    const url = `/api/shortest_paths?language-code=${database.languageCode}&dump-date=${database.dumpDate}&source=${sourceId}&target=${targetId}`;
    return this.get(url, Schema.Paths);
  };

  static randomPage = (languageCode: string) => {
    const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&list=random&rnnamespace=0&rnlimit=1`;
    return this.get(url, Schema.WikipediaRandom);
  };

  static titles = async (
    languageCode: string,
    pageIds: number[],
  ): Promise<Record<number, string>> => {
    const result: Record<number, string> = {};

    const unknownLocally = pageIds.filter((pageId) => {
      const cached = localStorage.getItem(pageId.toString());
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
          localStorage.setItem(id, title);
          result[parseInt(id)] = title;
        });
      }
    }

    await fetchTitles(unknownLocally);
    return result;
  };

  static suggestions = (
    languageCode: string,
    searchString: string,
    resultLimit: number,
    abort: AbortSignal,
  ) => {
    const url = `https://${languageCode}.wikipedia.org/w/rest.php/v1/search/title?q=${searchString}&limit=${resultLimit}`;
    return this.get(url, Schema.WikipediaSearch, abort);
  };
}

export type Page = z.infer<typeof Schema.Page>;
export type Database = z.infer<typeof Schema.Database>;
export type Paths = z.infer<typeof Schema.Paths>;

export abstract class Schema {
  static Id = z.number().int().nonnegative();
  static Title = z.string().min(1);

  static Page = z.object({
    id: this.Id,
    title: this.Title,
  });

  static Database = z.object({
    languageCode: z.string().min(1),
    dumpDate: z.string().min(1),
  });

  static Paths = z
    .object({
      source: this.Id,
      sourceIsRedirect: z.boolean(),
      target: this.Id,
      targetIsRedirect: z.boolean(),
      languageCode: z.string().min(1),
      links: z.record(
        z
          .string()
          .min(1)
          .transform((s) => parseInt(s)),
        z.array(this.Id),
      ),
      pathLengths: z.number().int().nonnegative(),
      pathCount: z.number().int().nonnegative(),
    })
    .transform(
      async (
        graph,
      ): Promise<{
        source: Page;
        sourceIsRedirect: boolean;
        target: Page;
        targetIsRedirect: boolean;
        languageCode: string;
        paths: Page[][];
        pathLengths: number;
        pathCount: number;
      }> => {
        const rawPaths = this.extractPaths(graph, 8);
        const titles = await Api.titles(
          graph.languageCode,
          flattenUnique(rawPaths),
        );
        const idToPage = (id: number) => ({ id: id, title: titles[id] });
        const paths = rawPaths.map((path) => path.map(idToPage));
        return {
          source: idToPage(graph.source),
          sourceIsRedirect: graph.sourceIsRedirect,
          target: idToPage(graph.target),
          targetIsRedirect: graph.targetIsRedirect,
          languageCode: graph.languageCode,
          paths: paths,
          pathLengths: graph.pathLengths,
          pathCount: graph.pathCount,
        };
      },
    );

  private static extractPaths = (
    graph: z.input<typeof this.Paths>,
    maxPaths: number,
  ): number[][] => {
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
  };

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
