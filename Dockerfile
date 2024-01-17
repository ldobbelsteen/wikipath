FROM node:bookworm AS web
WORKDIR /build
COPY web .
RUN npm ci
RUN npm run build

FROM rust:bookworm AS bin
WORKDIR /build
COPY . .
RUN cargo build --release

FROM debian:bookworm
RUN apt update && apt install -y ca-certificates && apt clean
COPY --from=web /build/dist /web/dist
COPY --from=bin /build/target/release/wikipath /usr/bin/wikipath
CMD ["wikipath", "serve"]
