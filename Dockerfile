FROM node:bullseye-slim AS web
WORKDIR /build
COPY web .
RUN npm ci
RUN npm run build

FROM rust:slim-bullseye AS bin
WORKDIR /build
COPY . .
COPY --from=web /build/dist /build/web/dist
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=bin /build/target/release/wikipath /usr/bin/wikipath
CMD ["wikipath", "serve"]
