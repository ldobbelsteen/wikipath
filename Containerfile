FROM docker.io/library/node:20-bookworm AS web
WORKDIR /build
COPY web/package*.json .
RUN npm ci
COPY web .
RUN npm run check
RUN npm run build

FROM docker.io/library/rust:1.81-bookworm AS bin
WORKDIR /build
COPY . .
RUN cargo build --release

FROM docker.io/library/debian:bookworm
STOPSIGNAL SIGINT
RUN apt update && apt install -y ca-certificates && apt clean
COPY --from=web /build/dist /web/dist
COPY --from=bin /build/target/release/wikipath /usr/bin/wikipath
ENTRYPOINT [ "wikipath" ]
CMD [ "serve" ]
