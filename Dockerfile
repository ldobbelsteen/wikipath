FROM node:alpine AS web-builder
WORKDIR /build
COPY . .
WORKDIR /build/web
RUN npm ci
RUN npm run lint
RUN npm run build

FROM golang:alpine AS bin-builder
RUN apk add --no-cache build-base
WORKDIR /build
COPY . .
RUN go test
RUN go build

FROM alpine
WORKDIR /
COPY --from=web-builder /build/web/dist /var/www/html
COPY --from=bin-builder /build/wikipath /usr/bin/wikipath
CMD ["wikipath", "serve", "--web", "/var/www/html"]
