FROM node:alpine AS web-builder
WORKDIR /build
COPY web/package.json .
COPY web/package-lock.json .
RUN npm install
COPY web .
RUN npm run build

FROM golang:alpine AS bin-builder
RUN apk add --no-cache build-base
WORKDIR /build
COPY . .
COPY --from=web-builder /build/dist web/dist
RUN go build

FROM alpine
COPY --from=bin-builder /build/wikipath /usr/bin/wikipath
WORKDIR /databases
CMD ["wikipath", "serve"]
