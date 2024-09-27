FROM docker.io/library/node:20-bookworm AS web
WORKDIR /build/web
COPY web .
RUN npm install
RUN npm run build

FROM docker.io/library/rust:1.81-bookworm AS bin
WORKDIR /build
COPY . .
COPY --from=web /build/web/dist /build/web/dist
RUN cargo build --release

FROM docker.io/library/debian:bookworm
STOPSIGNAL SIGINT
RUN apt update && apt install -y ca-certificates && apt clean
COPY --from=bin /build/target/release/wikipath /usr/bin/wikipath
ENTRYPOINT [ "wikipath" ]
CMD [ "serve" ]
