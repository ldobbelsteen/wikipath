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
RUN apt update && apt install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=bin /build/target/release/wikipath /usr/bin/wikipath
CMD ["wikipath", "serve"]
