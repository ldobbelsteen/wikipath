import { z } from "zod";
import { flattenUnique, pseudoRandomShuffle } from "./misc";

export abstract class HTTP {
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
    langCode: string,
    sourceId: number,
    targetId: number
  ) => {
    const url = `/api/shortest_paths?language=${langCode}&source=${sourceId}&target=${targetId}`;
    return this.get(url, Schema.Paths);
  };

  static randomPage = (langCode: string) => {
    const url = `https://${langCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&list=random&rnnamespace=0&rnlimit=1`;
    return this.get(url, Schema.WikipediaRandom);
  };

  static pageTitles = async (
    langCode: string,
    pageIds: number[]
  ): Promise<Record<number, string>> => {
    if (pageIds.length > 50) {
      const left = pageIds.slice(0, 50);
      const right = pageIds.slice(50);
      const leftResult = await this.pageTitles(langCode, left);
      const rightResult = await this.pageTitles(langCode, right);
      return Promise.resolve(Object.assign({}, leftResult, rightResult));
    }
    const delimitedPages = pageIds.join("|");
    const url = `https://${langCode}.wikipedia.org/w/api.php?origin=*&action=query&format=json&pageids=${delimitedPages}`;
    return this.get(url, Schema.WikipediaTitles);
  };

  static suggestions = (
    langCode: string,
    searchString: string,
    resultLimit: number,
    abort: AbortSignal
  ) => {
    const url = `https://${langCode}.wikipedia.org/w/api.php?origin=*&action=query&list=prefixsearch&pslimit=${resultLimit}&pssearch=${searchString}&format=json`;
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
    langCode: z.string().min(1),
    dumpDate: z.string().min(1),
  });

  static Paths = z
    .object({
      source: this.Id,
      sourceIsRedir: z.boolean(),
      target: this.Id,
      targetIsRedir: z.boolean(),
      langCode: z.string().min(1),
      links: z.record(
        z
          .string()
          .min(1)
          .transform((s) => parseInt(s)),
        z.array(this.Id)
      ),
      pathLength: z.number().int().nonnegative(),
      pathCount: z.number().int().nonnegative(),
    })
    .transform(
      async (
        graph
      ): Promise<{
        source: Page;
        sourceIsRedir: boolean;
        target: Page;
        targetIsRedir: boolean;
        langCode: string;
        paths: Page[][];
        pathLength: number;
        pathCount: number;
      }> => {
        const rawPaths = this.extractPaths(graph, 8);
        const titles = await HTTP.pageTitles(
          graph.langCode,
          flattenUnique(rawPaths)
        );
        const idToPage = (id: number) => ({ id: id, title: titles[id] });
        const paths = rawPaths.map((path) => path.map(idToPage));
        return {
          source: idToPage(graph.source),
          sourceIsRedir: graph.sourceIsRedir,
          target: idToPage(graph.target),
          targetIsRedir: graph.targetIsRedir,
          langCode: graph.langCode,
          paths: paths,
          pathLength: graph.pathLength,
          pathCount: graph.pathCount,
        };
      }
    );

  private static extractPaths = (
    graph: z.input<typeof this.Paths>,
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
