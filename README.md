# Wikipath

[Wikipath](https://wikipath.dobbel.dev) is a website with which the shortest path between any two Wikipedia articles can be found. The shortest path is the least number of clicks on links necessary to reach a target page from a starting page. The project is heavily inspired by the [Six Degrees of Wikipedia](https://www.sixdegreesofwikipedia.com) project, but aims at better performance and supporting multiple languages.

## Building

The project consists of a backend and a frontend. To build, `npm` and `cargo` are required. First, to build the frontend, run:

```
cd web
npm install
npm run build
```

Then, to build the backend, back in the root of the project run:

```
cargo build --release
```

This will build a binary in the `./target/release` directory.

## Databases

You can have one or more databases served at a time, one for each language. A database contains all redirects and hyperlinks of a Wikipedia, which are used to search for the shortest paths. Databases can be constructed using Wikipedia's periodic dumps (for example, see [here](https://dumps.wikimedia.org/) for the English Wikipedia dumps). To build them yourself, the `build` subcommand is included. For more information, use its `--help` option.

## Serving

Once the database(s) have been built, the `serve` subcommand can be used to serve the frontend along with an API for searching the databases. In contrast to the database build process, this is very light on resources. For more information, use the `--help` option.

## Docker

There is a Containerfile included with which a container image can be built that contains the binary. There are pre-built images available on the GitHub Packages of this repository.

An example of how the image can be used to serve databases:

```
docker run -d \
    --name wikipath \
    --restart always \
    --publish 1789:1789 \
    --volume /path/to/db/directory:/databases:ro \
    ghcr.io/ldobbelsteen/wikipath
```

An example of how the image can be used to build databases:

```
docker run --rm -it \
    --volume /path/to/db/directory:/databases \
    --volume /path/to/dump/cache/directory:/dumps \
    ghcr.io/ldobbelsteen/wikipath build --languages es,de,fr --date 20240501
```
