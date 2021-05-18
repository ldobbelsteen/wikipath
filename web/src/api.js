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
  try {
    const response = await fetch("/databases");
    if (!response.ok)
      throw `Failed to get available databases (${response.status})`;
    const databases = await response.json();
    if (!Array.isArray(databases)) throw "Unexpected database format";
    if (databases.length === 0) throw "No databases found";
    return databases;
  } catch (err) {
    console.error(err);
    return [];
  }
}

export async function getRandomTitle(language) {
  try {
    if (!language) throw "Language not specified";
    const response = await fetch(`/random?language=${language}`);
    if (!response.ok) throw `Failed to get random title (${response.status})`;
    const title = await response.text();
    if (typeof title != "string") throw "Unexpected title format";
    return title;
  } catch (err) {
    console.error(err);
    return "Error";
  }
}

export async function getShortestPaths(source, target, language) {
  try {
    if (!source) return "source";
    if (!target) return "target";
    if (!language) throw "Language not specified";
    const response = await fetch(
      `/paths?source=${source}&target=${target}&language=${language}`
    );
    if (!response.ok) {
      const message = (await response.text()).trim();
      if (message === "source page not found") {
        return "source";
      }
      if (message === "target page not found") {
        return "target";
      }
      throw `Failed to get shortest paths (${response.status})`;
    }
    const paths = await response.json();
    if (!Array.isArray(paths)) throw "Invalid paths format";
    return paths;
  } catch (err) {
    console.error(err);
    return [];
  }
}

export async function getSuggestions(input, language) {
  try {
    if (!input) throw "No input specified";
    if (!language) throw "No language specified";
    const url = `https://${language}.wikipedia.org/w/api.php?
			origin=*&
			format=json&
			action=opensearch&
			search=${input}&
			namespace=0&
			limit=5`;
    const response = await fetch(url, {
      headers: {
        "Api-User-Agent":
          "Wikipath/1.0 (https://github.com/ldobbelsteen/wikipath/)",
      },
    });
    if (!response.ok)
      throw `Failed to get search suggestions (${response.status})`;
    const result = await response.json();
    if (!Array.isArray(result) || !Array.isArray(result[1]))
      throw "Invalid search suggestions format";
    return result[1];
  } catch (err) {
    console.error(err);
    return [];
  }
}
