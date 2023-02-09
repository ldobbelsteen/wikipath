FROM node:bullseye AS web
WORKDIR /build
COPY web .
RUN npm ci
RUN npm run build

FROM rust:bullseye AS bin
WORKDIR /build
COPY . .
COPY --from=web /build/dist /build/web/dist
RUN cargo build --release

FROM debian:bullseye
COPY --from=bin /build/target/release/wikipath /usr/bin/wikipath
CMD ["wikipath", "serve"]
