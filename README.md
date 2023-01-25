# Wikipath

[Wikipath](https://wikipath.dobbel.dev) is a website with which the shortest path between any two Wikipedia articles can be found. The shortest path is the least number of clicks on links necessary to reach a target page from a starting page. The project is heavily inspired by the [Six Degrees of Wikipedia](https://www.sixdegreesofwikipedia.com) project, but aims at better performance and supporting any language.

## Building

This project consists of two main parts; the frontend and the backend. To build the frontend, head into the `web` directory and simply build the `npm` project (make sure you have `node` and `npm` installed):

```
npm install
npm run build
```

Then to build the backend, go back to the root directory and build by running (make sure you have `cargo` installed):

```
cargo build --release
```

This will build a binary in the `target/release` directory with the frontend embedded.

## Data

The application will require at least one database containing all of Wikipedia's pages and hyperlinks to search for the shortest paths. Wikipedia periodically [dumps its data](https://dumps.wikimedia.org/), which can be used to build such a database. To do this yourself, the `build` subcommand is included.

```
wikipath build
    [--databases <directory>]
    [--dumps <directory>]
    [--language <language-tag>]
```

`--databases` specifies the directory to output the database to. Defaults to `./databases`.

`--dumps` specifies the directory to download the raw dumps into. Defaults to `./dumps`.

`--language` specifies which Wikipedia language to create a database of. A list of all Wikipedia languages can be found [here](https://en.wikipedia.org/wiki/List_of_Wikipedias). The language should be specified in the language code of the website (e.g. `en` for English, `de` for German, etc.). Defaults to `en`.

This command does everything for you; it downloads the latest dumps, parses them and ingests them into a database. This process is extremely optimized, but can still take a long time depending on your machine's processing power and memory. Different Wikipedia languages also have very differing numbers of articles, which also hugely influences the build time.

## Serving

Once the database(s) have been built (you can build databases of as many languages as you want and all of them will be available on the web interface), the `serve` subcommand will serve the HTTP web interface. In contrast to the build process, this is very light on processing power, memory and network usage.

```
wikipath serve
    [--databases <directory>]
    [--port <port-number>]
```

`--databases` specifies the directory where the database(s) is/are located. Defaults to `./databases`.

`--port` specifies the port on which the web interface is served. Defaults to `1789`.

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
