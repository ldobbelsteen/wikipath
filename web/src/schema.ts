import { z } from "zod";

export interface Page {
  id: number;
  title: string;
}

export interface Database {
  languageCode: string;
  dateCode: string;
}

export interface Paths {
  languageCode: string;
  dateCode: string;

  source: Page;
  sourceIsRedirect: boolean;
  target: Page;
  targetIsRedirect: boolean;

  paths: Page[][];
  length: number;
  count: number;
}

const IdSchema = z.number().int().nonnegative();
const TitleSchema = z.string().min(1);

const PageSchema = z.object({
  id: IdSchema,
  title: TitleSchema,
});

export const DatabaseSchema = z.object({
  languageCode: z.string().min(1),
  dateCode: z.string().min(1),
});

export const PathsSchema = z.object({
  source: IdSchema,
  sourceIsRedirect: z.boolean(),
  target: IdSchema,
  targetIsRedirect: z.boolean(),
  links: z.record(
    z
      .string()
      .min(1)
      .transform((s) => Number.parseInt(s, 10)),
    z.array(IdSchema),
  ),
  languageCode: z.string().min(1),
  dateCode: z.string().min(1),
  length: z.number().int().nonnegative(),
  count: z.number().int().nonnegative(),
});

export const WikipediaRandomSchema = z
  .object({
    query: z.object({
      random: z.array(PageSchema).length(1),
    }),
  })
  .transform((obj) => obj.query.random[0]);

export const WikipediaTitlesSchema = z
  .object({
    query: z.object({
      pages: z.record(
        z.string().min(1),
        z.object({
          pageid: IdSchema,
          title: TitleSchema,
        }),
      ),
    }),
  })
  .transform((obj) =>
    Object.values(obj.query.pages).reduce<Record<number, string>>(
      (record, page) => {
        record[page.pageid] = page.title;
        return record;
      },
      {} as Record<number, string>,
    ),
  );

export const WikipediaSearchSchema = z
  .object({
    pages: z.array(
      z.object({
        id: IdSchema,
        title: TitleSchema,
      }),
    ),
  })
  .transform((obj) => obj.pages);
