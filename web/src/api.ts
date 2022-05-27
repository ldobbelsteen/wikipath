import { z } from "zod";
import { flattenUnique, pseudoRandomShuffle } from "./misc";

export abstract class HTTP {
  private static prefix = ""; // e.g. http://localhost:1789
  private static headers = {
    "Api-User-Agent":
      "Wikipath/1.0 (https://github.com/ldobbelsteen/wikipath/)",
  };

  private static get = async <T, U>(
    url: string,
    schema: z.Schema<T, z.ZodTypeDef, U>,
    abort?: AbortSignal
  ): Promise<T> => {
    const res = await fetch(url, {
      signal: abort,
      headers: this.headers,
      method: "GET",
    });
    if (res.ok) {
      const parse = await schema.safeParseAsync(await res.json());
      if (parse.success) {
        return parse.data;
      } else {
        return Promise.reject(parse.error);
      }
    } else {
      return Promise.reject(await res.text());
    }
  };

  static getDatabases = () => {
    const url = this.prefix + "/databases";
    return this.get(url, z.array(Schema.Database));
  };

  static getGraph = (
    languageCode: string,
    sourceId: number,
    targetId: number
  ) => {
    const url =
      this.prefix +
      `/paths?language=${languageCode}&source=${sourceId}&target=${targetId}`;
    return this.get(url, Schema.Graph);
  };

  static getRandomPage = (languageCode: string) => {
    const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&list=random&rnnamespace=0&rnlimit=1`;
    return this.get(url, Schema.WikipediaRandom);
  };

  static getPageTitles = async (
    languageCode: string,
    pageIds: number[]
  ): Promise<Record<number, string>> => {
    if (pageIds.length > 50) {
      const left = pageIds.slice(0, 50);
      const right = pageIds.slice(50);
      const leftResult = await this.getPageTitles(languageCode, left);
      const rightResult = await this.getPageTitles(languageCode, right);
      return Promise.resolve(Object.assign({}, leftResult, rightResult));
    }
    const delimitedPages = pageIds.join("|");
    const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&pageids=${delimitedPages}`;
    return this.get(url, Schema.WikipediaTitles);
  };

  static getSuggestions = (
    languageCode: string,
    searchString: string,
    resultLimit: number,
    abort: AbortSignal
  ) => {
    const url = `https://${languageCode}.wikipedia.org/w/api.php?origin=*&action=query&list=prefixsearch&pslimit=${resultLimit}&pssearch=${searchString}&format=json`;
    return this.get(url, Schema.WikipediaSearch, abort);
  };
}

export type Page = z.infer<typeof Schema.Page>;
export type Database = z.infer<typeof Schema.Database>;
export type Graph = z.infer<typeof Schema.Graph>;

export abstract class Schema {
  static Id = z.number().int().nonnegative();
  static Title = z.string().min(1);

  static Page = z.object({
    id: this.Id,
    title: this.Title,
  });

  static Database = z.object({
    dumpDate: z.string().min(1),
    buildDate: z.string().min(1),
    languageCode: z.string().min(1),
    languageName: z.string().min(1),
    largestPageId: this.Id,
  });

  static Graph = z
    .object({
      languageCode: z.string().min(1),
      links: z.record(
        z
          .string()
          .min(1)
          .transform((s) => parseInt(s)),
        z.array(this.Id)
      ),
      pathCount: z.number().int().nonnegative(),
      pathDegrees: z.number().int().nonnegative(),
      sourceId: this.Id,
      targetId: this.Id,
      sourceIsRedir: z.boolean(),
      targetIsRedir: z.boolean(),
    })
    .transform(
      async (
        graph
      ): Promise<{
        languageCode: string;
        pathDegrees: number;
        pathCount: number;
        paths: Page[][];
        sourcePage: Page;
        targetPage: Page;
        sourceIsRedir: boolean;
        targetIsRedir: boolean;
      }> => {
        const rawPaths = this.extractPaths(graph, 8);
        const titles = await HTTP.getPageTitles(
          graph.languageCode,
          flattenUnique(rawPaths)
        );
        const idToPage = (id: number) => ({ id: id, title: titles[id] });
        const paths = rawPaths.map((path) => path.map(idToPage));
        return {
          languageCode: graph.languageCode,
          pathDegrees: graph.pathDegrees,
          pathCount: graph.pathCount,
          paths: paths,
          sourcePage: idToPage(graph.sourceId),
          targetPage: idToPage(graph.targetId),
          sourceIsRedir: graph.sourceIsRedir,
          targetIsRedir: graph.targetIsRedir,
        };
      }
    );

  private static extractPaths = (
    graph: z.input<typeof this.Graph>,
    maxPaths: number
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
    recurse(graph.sourceId, [graph.sourceId]);
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
          })
        ),
      }),
    })
    .transform((obj) =>
      Object.values(obj.query.pages).reduce(
        (record, page) => ({ ...record, [page.pageid]: page.title }),
        {} as Record<number, string>
      )
    );

  static WikipediaSearch = z
    .object({
      query: z.object({
        prefixsearch: z.array(
          z
            .object({
              pageid: this.Id,
              title: this.Title,
            })
            .transform((p) => ({ id: p.pageid, title: p.title }))
        ),
      }),
    })
    .transform((obj) => obj.query.prefixsearch);
}
