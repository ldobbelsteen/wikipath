# Wikipath

[Wikipath](https://wikipath.dobbel.dev) is a website with which the shortest path between any two Wikipedia articles can be found. The shortest path is the least number of clicks on links necessary to reach a target page from a starting page. The project is heavily inspired by the [Six Degrees of Wikipedia](https://www.sixdegreesofwikipedia.com) project, but aims at better performance and supporting any language. The primary raison d'être of this project is personally learning Go and web development.

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

This will build a binary in the current directory with the front-end in the `web/dist` directory. The binary requires the front-end to be present in the `web/dist` directory or for it to be specified to reside elsewhere.

## Data

The application will require at least one database containing all of Wikipedia's pages and hyperlinks to search for the shortest paths. Wikipedia periodically [dumps its data](https://dumps.wikimedia.org/), which can be used to build such a database. To do this yourself, the `build` subcommand is included.

```
wikipath build
    [--output <directory>]
    [--dumps <directory>]
    [--mirror <mirror-url>]
    [--language <language-tag>]
```

`--output` specifies the directory to output the database to. Defaults to `./databases`

`--dumps` specifies the directory to download the raw dumps into. Defaults to `./dumps`

`--mirror` specifies the dump mirror to download the raw dumps from. As mentioned on the official dump website, please consider using a mirror or hosting your own to make sure the (free) official mirror can stay available and performant for everyone. Make sure the URL points to the root of the actual dump (e.g. the directory containing the actual `wiki` files) and has a schema (e.g. `https://` in front of it). Defaults to the official `https://dumps.wikimedia.org`

`--language` specifies which Wikipedia language to create a database of. A list of all Wikipedia languages can be found [here](https://en.wikipedia.org/wiki/List_of_Wikipedias). The language should be specified in the language code of the website (e.g. `en` for English, `de` for German, etc.) Defaults to `en`

This command does everything for you; it downloads the latest dumps, parses them and ingests them into an SQLite database. This process is extremely optimized, but can still take a long time depending on your machine's processing power and memory. Different Wikipedia languages also have very differing numbers of articles, which also hugely influences the build time.

There is a minimum amount of system memory required in the build process, as part of the dump has to be kept in-memory. To give an example, the English database requires at least around 30GB of memory, meaning you need a system with about 32GB of memory minimum. It is, however, always better to have more memory than necessary, because it relieves GC pressure and thus increases speed.

Also keep in mind that the compressed dump files stay on disk to prevent re-downloads on re-builds and as such will require a minimum free disk space equal to the sum of the `pagelinks.sql.gz`, `page.sql.gz` and `redirect.sql.gz` dumps (approx. 9GB for the English database) on top of the size of the final database (approx. 6GB for the English database).

To give an idea of how long the build process takes; a computer with 6 cores and 32GB of memory takes around 30 minutes (excluding download times) to complete a build of the English Wikipedia.

## Serving

Once the database(s) have been built (you can build databases of as many languages as you want and all of them will be available on the web interface), the `serve` subcommand will serve the HTTP web interface on port `1789`. In contrast to the build process, this is very light on processing power, memory and network usage.

```
wikipath serve
    [--databases <directory>]
    [--web <directory>]
```

`--databases` specifies the directory where the database(s) is/are located. Defaults to `./databases`

`--web` specifies the directory where the bundled web files reside. Defaults to `web/dist`

## Docker

There is a Dockerfile included with which an image can be built that can serve databases. There are pre-built images available on the GitHub Packages of this repository. An example of how the image can be used:

```
docker run \
    --detach \
    --restart always \
    --publish 1789:1789 \
    --volume /path/to/db/directory:/databases \
    ghcr.io/ldobbelsteen/wikipath
```
