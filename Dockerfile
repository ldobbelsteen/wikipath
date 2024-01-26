FROM node:bookworm AS web
WORKDIR /build/web
COPY web .
RUN npm install
RUN npm run build

FROM rust:bookworm AS bin
WORKDIR /build
COPY . .
COPY --from=web /build/web/dist /build/web/dist
ENV SKIP_NPM=1
RUN cargo build --release

FROM debian:bookworm
RUN apt update && apt install -y ca-certificates && apt clean
COPY --from=bin /build/target/release/wikipath /usr/bin/wikipath
CMD ["wikipath", "serve"]
