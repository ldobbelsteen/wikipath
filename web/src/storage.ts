/** Store a page's title in localStorage as cache. */
export function storeTitle(pageId: string, pageTitle: string) {
  const existing = localStorage.getItem(pageId);
  if (existing === null || existing !== pageTitle) {
    localStorage.setItem(pageId, pageTitle);
  }
}

/** Get a page's title from the localStorage cache. */
export function getTitle(pageId: string) {
  return localStorage.getItem(pageId);
}
