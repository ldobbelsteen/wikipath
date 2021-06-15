# Wikipath
[Wikipath](https://wikipath.dobbel.dev) is a website with which the shortest path between any two Wikipedia articles can be found. The shortest path is the least number of clicks on links necessary to reach a target page from a source page. The project is heavily inspired by the [Six Degrees of Wikipedia](https://www.sixdegreesofwikipedia.com) project, but aims at better performance and supporting any language. The primary raison d'être of this project is personally learning Go and web development.

## Building
This project consists of two main parts; the front-end and the back-end. To build the front-end, head into the `web` directory and simply build the NPM project (make sure you have Node and NPM installed):

```
npm install
npm run build
```

Then to build the back-end, go back to the root directory and build by running:

```
go build
```

This will build a binary in the current directory with the front-end embedded into it, meaning it's fully portable. This also means the front-end needs to be built before building the Go binary. This project depends on [go-sqlite3](https://github.com/mattn/go-sqlite3) which is a `cgo` package and as such requires `gcc`.

## Data
The application will require at least one database containing all of Wikipedia's pages and hyperlinks to search for the shortest paths. Wikipedia periodically [dumps its databases](https://dumps.wikimedia.org/), which can be used to build such a database. To do this yourself, the `build` subcommand is included:

```
wikipath build
    [--output <directory>]
    [--dumps <directory>]
    [--mirror <mirror-url>]
    [--language <language-tag>]
    [--memory <percentage>]
```

`--output` specifies the directory to output the database to. Defaults to the current directory

`--dumps` specifies the directory to download the raw dumps into. Defaults to `./dumps`

`--mirror` specifies the dump mirror to download the raw dumps from. As mentioned on the official dump website, please consider using a mirror or hosting your own to make sure the (free) official mirror can stay performant for everyone. Make sure the URL points to the root of the actual dump (e.g. the directory containing the `__wiki` files) and has a schema (e.g. `https://` in front of it). Defaults to the official `https://dumps.wikimedia.org`

`--language` specifies which Wikipedia language to create a database of. A list of all Wikipedia languages can be found [here](https://en.wikipedia.org/wiki/List_of_Wikipedias). The language should be specified in the language code of the website (e.g. `en` for English, `de` for German, etc.) Defaults to `en`

`--memory` specificies the maximum memory usage the build process can use in the percentage of total system memory it is allowed. The higher the value, the faster the build process will be. This value may be exceeded, so make sure there is some headroom. Defaults to `70`

This command does everything for you; it downloads the latest dumps, parses them and ingests them into an SQLite database. Be aware that this process takes a long time depending on your machine's processing power and memory size. Different Wikipedia languages also have very differing numbers of articles, which also hugely influences the build time. There is a minimum amount of memory required, as part of the dump has to be kept in-memory during this process (approx. 12GB for the English database). Any remaining memory from the `memory` parameter is used to cache links, which greatly improves performance as less database inserts are needed. This means that about 16GB of system memory is the absolute minimum if you are building the English database. Also keep in mind that the compressed dump files stay on disk to prevent re-downloads on re-builds and as such will require a minimum free disk space equal to the sum of the `pagelinks.sql.gz`, `page.sql.gz` and `redirect.sql.gz` dumps (approx. 9GB for the English database) on top of the size of the final database (approx. 12GB for the English database). To give an idea of how long the build process takes; a computer with 6 cores and 24GB of memory takes around 40 minutes (excluding download times) to complete a build of the English Wikipedia.

## Serving
Once the database(s) have been built (you can build databases of as many languages as you want and all of them will be available on the web interface), the `serve` subcommand will serve the HTTP web interface on port `1789`:

```
wikipath serve
    [--databases <directory>]
    [--cache <megabytes>]
```

`--databases` specifies the directory where the database(s) is/are located. Defaults to the current directory

`--cache` specifies the maximum size of the shortest path search results cache in megabytes for each database. This feature caches searches that took longer than 2 seconds to finish. Defaults to `32`

## Docker
There is a Dockerfile included with which an image can be built that can serve databases. You can build your own as follows:

```
docker build --tag wikipath https://github.com/ldobbelsteen/wikipath.git
```

Images are pre-built by GitHub Actions and can be found [here](https://hub.docker.com/r/ldobbelsteen/wikipath). An example of how the image can then be used:

```
docker run \
    --detach \
    --restart always \
    --volume /path/to/db/directory:/databases \
    ldobbelsteen/wikipath
```
