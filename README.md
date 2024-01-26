# Wikipath

[Wikipath](https://wikipath.dobbel.dev) is a website with which the shortest
path between any two Wikipedia articles can be found. The shortest path is the
least number of clicks on links necessary to reach a target page from a starting
page. The project is heavily inspired by the
[Six Degrees of Wikipedia](https://www.sixdegreesofwikipedia.com) project, but
aims at better performance and supporting multiple languages.

## Building

The project consists of a backend and a frontend. To build, `npm` and `cargo`
are required. Then, run the following command:

```
cargo build --release
```

This will build a binary in the `./target/release` directory with the frontend
embedded.

## Databases

You can have one or more databases served at a time, one for each language. A
database contains all redirects and hyperlinks of a Wikipedia, which are used to
search for the shortest paths. Databases can be constructed using Wikipedia's
[periodic dumps](https://dumps.wikimedia.org/). To build them yourself, the
`build` subcommand is included. For more information, use its `--help` option.

## Serving

Once the database(s) have been built, the `serve` subcommand can be used to
serve the frontend along with an API for searching the databases. In contrast to
the database build process, this is very light on resources. For more
information, use the `--help` option.

## Docker

There is a Dockerfile included with which an image can be built that can serve
databases. There are pre-built images available on the GitHub Packages of this
repository. An example of how the image can be used:

```
docker run \
    --detach \
    --restart always \
    --publish 1789:1789 \
    --volume /path/to/db/directory:/databases \
    ghcr.io/ldobbelsteen/wikipath
```
