FROM docker.io/node:22-alpine AS web
WORKDIR /build
COPY web/package*.json .
RUN npm ci
COPY web .
RUN npm run check
RUN npm run build

FROM docker.io/rust:1.89-bookworm AS bin
WORKDIR /build
COPY . .
RUN cargo build --release

FROM docker.io/debian:bookworm-slim
RUN apt update && apt install -y ca-certificates && apt clean
COPY --from=web /build/dist /web/dist
COPY --from=bin /build/target/release/wikipath /usr/bin/wikipath
ENTRYPOINT [ "wikipath" ]
STOPSIGNAL SIGINT
CMD [ "serve" ]
