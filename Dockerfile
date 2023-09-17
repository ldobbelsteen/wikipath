FROM node:bookworm AS web
WORKDIR /build
COPY web .
RUN npm ci
RUN npm run build

FROM rust:bookworm AS bin
WORKDIR /build
COPY . .
COPY --from=web /build/dist /build/web/dist
RUN cargo build --release

FROM debian:bookworm
COPY --from=bin /build/target/release/wikipath /usr/bin/wikipath
CMD ["wikipath", "serve"]
