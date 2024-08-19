/** Flatten a two-dimensional array, removing any duplicates */
export const flattenUnique = <T>(arr: T[][]): T[] => {
  const set = new Set<T>();
  arr.forEach((subArr) => {
    subArr.forEach((element) => {
      set.add(element);
    });
  });
  return Array.from(set);
};

/** Compare two strings ignoring casing, accents, etc. */
export const weakStringEquals = (a: string, b: string): boolean => {
  return a.localeCompare(b, undefined, { sensitivity: "base" }) === 0;
};
