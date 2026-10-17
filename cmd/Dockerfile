# Stage 1: Build

FROM golang:1.25.5-alpine AS build

WORKDIR /app

COPY go.mod go.sum* ./

RUN go mod download

COPY . .

RUN go build -o build.bin ./cmd

# Stage 2: Kraft

FROM golang:1.26-alpine AS kraft

ARG KRAFTKIT_REPO=https://github.com/unikraft/kraftkit
ARG KRAFTKIT_REF=v0.12.16-13-g22e305d1

RUN apk add --no-cache git

RUN git clone --depth 1 --branch "${KRAFTKIT_REF}" "${KRAFTKIT_REPO}" /kraftkit \
    && cd /kraftkit \
    && CGO_ENABLED=0 go build -o /kraft ./cmd/kraft

# Stage 3: Run

FROM kraftkit.sh/base:latest AS run

COPY --from=kraft /kraft /usr/local/bin/kraft

WORKDIR /app

COPY cmd/test_data ./test_data

COPY --from=build /app/build.bin /usr/bin/runner

ENTRYPOINT ["/usr/bin/runner"]