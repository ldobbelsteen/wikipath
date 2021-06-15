export function getUrlParameter(key) {
  return new URLSearchParams(window.location.search).get(key);
}

export function setUrlParameters(obj) {
  let result = "?";
  for (const key in obj) {
    result += key + "=" + obj[key] + "&";
  }
  history.replaceState(null, document.title, result.slice(0, -1));
}

export async function getAvailableDatabases() {
  const res = await fetch("/databases", {
    method: "POST",
  });
  const databases = await res.json();
  return databases;
}

export async function getRandomTitle(language) {
  const res = await fetch(`/random/${language}`, {
    method: "POST",
  });
  const title = await res.text();
  return title;
}

export async function getShortestPaths(source, target, language) {
  if (!source) return "source";
  if (!target) return "target";
  const res = await fetch(`/paths/${language}/${source}/${target}`, {
    method: "POST",
  });
  if (!res.ok) {
    const message = (await res.text()).trim();
    if (message === "source page not found") {
      return "source";
    }
    if (message === "target page not found") {
      return "target";
    }
    console.log(message);
  }
  const paths = await res.json();
  return paths;
}

export async function getSuggestions(input, language) {
  const url = `https://${language}.wikipedia.org/w/api.php?origin=*&format=json&action=opensearch&search=${input}&namespace=0&limit=5`;
  const res = await fetch(url, {
    headers: {
      "Api-User-Agent":
        "Wikipath/1.0 (https://github.com/ldobbelsteen/wikipath/)",
    },
  });
  const suggestions = await res.json();
  return suggestions[1];
}
