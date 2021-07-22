FROM node:alpine AS web-builder
WORKDIR /build
COPY web/package.json web/package-lock.json ./
RUN npm install
COPY web .
RUN npm run build

FROM golang:alpine AS bin-builder
WORKDIR /build
RUN apk add --no-cache build-base
COPY . .
RUN go build

FROM alpine
WORKDIR /
COPY --from=web-builder /build/dist /var/www/html
COPY --from=bin-builder /build/wikipath /usr/bin/wikipath
CMD ["wikipath", "serve", "--web", "/var/www/html"]
