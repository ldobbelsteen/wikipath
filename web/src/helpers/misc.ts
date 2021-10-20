// Run a deterministic pseudo random shuffle on an array, in-place
export function pseudoRandomShuffle<T>(array: T[]): T[] {
  let seed = 1;

  const pseudo = () => {
    const x = Math.sin(seed++) * 10000;
    return x - Math.floor(x);
  };

  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(pseudo() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }

  return array;
}

// Compare two strings ignoring casing, accents, etc.
export function weakStringEquals(a: string, b: string): boolean {
  return a.localeCompare(b, undefined, { sensitivity: "base" }) === 0;
}

// Flatten a two-dimensional array, removing any duplicates
export function flattenUnique<T>(array: T[][]): T[] {
  const set: Set<T> = new Set();
  array.forEach((subArray) => {
    subArray.forEach((element) => {
      set.add(element);
    });
  });
  return Array.from(set);
}
