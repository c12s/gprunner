# Runner

Runner for Overlay data mesh charts. Pulls charts from [starmap](https://github.com/c12s/starmap) and runs their layers as Unikraft unikernels.

### Application configuration

| Parameter | Description | Default value |
|--|--|--|
| STARMAP_ADDR | Address (host:port) of the starmap registry gRPC server. Required. | - |
| GPRUNNER_PORT | The port number at which the runner gRPC server is listening. | 50052 |
| GPRUNNER_CACHE | Directory where images, build files and the SQLite database are stored. | /tmp/gprunner-cache |
| GPRUNNER_BROKER | Hostname of the NATS broker. Compose appends NATS_CLIENT_PORT to it. | nats:4222 |
| GPRUNNER_BROKER_SUBNET | Subnet of the bridge layers use to reach the broker. Its address is the gateway passed to layers as MQ_ADDR. | 172.200.0.1/16 |
| NATS_CLIENT_PORT | The port number of the NATS client connection. | 4222 |
| NATS_MONITOR_PORT | The port number of the NATS monitoring endpoint. | 8222 |
| KRAFTKIT_BUILDKIT_HOST | BuildKit socket kraft uses to build root filesystems. | unix:///run/buildkit/buildkitd.sock |
| KRAFTKIT_NO_PROMPT | Disables interactive kraft prompts. | true |
| KRAFTKIT_LOG_TYPE | kraft log output format. | basic |
| KRAFTKIT_NO_COLOR | Disables colored kraft output. | true |
| KRAFTKIT_NO_CHECK_UPDATES | Disables kraft update checks. | true |

.env example (`cmd/.env.example`):

    GPRUNNER_CACHE=/app/data

    KRAFTKIT_BUILDKIT_HOST=unix:///run/buildkit/buildkitd.sock
    KRAFTKIT_NO_PROMPT=true
    KRAFTKIT_LOG_TYPE=basic
    KRAFTKIT_NO_COLOR=true
    KRAFTKIT_NO_CHECK_UPDATES=true

    NATS_CLIENT_PORT=4222
    NATS_MONITOR_PORT=8222

    # hostname only; compose appends NATS_CLIENT_PORT
    GPRUNNER_BROKER=nats
    GPRUNNER_BROKER_SUBNET=172.200.0.1/24
    GPRUNNER_PORT=50052

    STARMAP_ADDR=starmap:9002

## Running with Docker Compose

### Prerequisites

- Linux host with KVM (`/dev/kvm`) and `/dev/net/tun`
- Docker with Docker Compose
- The `starmap-net` Docker network, which gprunner and starmap share:

      docker network create starmap-net

### Starting starmap

**Start starmap before gprunner.** gprunner pulls every chart from starmap and reaches it at `STARMAP_ADDR` (`starmap:9002`) over `starmap-net`. From the root of the [starmap](https://github.com/c12s/starmap) repository, with its `.env` in place:

    docker compose up -d --build

### Starting gprunner

Once starmap is up, run this from the `cmd` directory of this repository:

    cd cmd
    cp .env.example .env
    docker compose up --build

This will automatically run:

    - gprunner (runner service)
    - NATS (message broker)
    - BuildKit (used by kraft for builds)

The runner is ready once it logs `runner listening`. Images and the database are stored in `~/data` on the host.

### Testing

To try it out:

1. Store the example chart in starmap with `/PutChart`: `cmd/test_data/starchart.json`.
2. Call `/Instantiate` on gprunner with `pull: true`.
3. Check the running layers with `docker compose exec runner kraft ps`.
4. Stop the chart with `/Kill`.


## gRPC Endpoints

### /Pull

The endpoint for fetching a chart from starmap and storing it locally, replacing any earlier copy. Layer selectors choose the version and architecture of each layer image. If no version of an image layer matches its selector, the pull fails with `NotFound` and the earlier copy is kept. Nothing is started.

#### Request body

```proto
message PullReq {
  ChartRef ref = 1;
  map<string, LayerSelector> layers = 2;
}

message ChartRef {
  string name = 1;
  string namespace = 2;
  string maintainer = 3;
  string schemaVersion = 4;
}

// keyed by image name without the tag, both fields optional:
// an empty semver means latest, an empty arch the multi-platform index
message LayerSelector {
  string semver = 1;
  string arch = 2;
}
```

#### Response - 0 OK

```proto
message Empty {}
```

### /Instantiate

The endpoint for starting a chart. Before anything starts, every layer is resolved: links and volumes are validated, then the layer image is pulled or built (or taken from the cache). Layers then start in groups, in order: stored procedures, event triggers, events, with the layers of each group starting concurrently. If any layer fails, the whole chart is torn down. With `pull: true`, the chart is pulled from starmap first, using the given layer selectors. If `pull: false` the layer selectors are ignored and we use whatever layer versions are persisted in the cache.

#### Request body

```proto
message InstantiateReq {
  ChartRef ref = 1;
  bool pull = 2;
  map<string, LayerSelector> layers = 3;
}
```

#### Response - 0 OK

```proto
message Empty {}
```

### /Kill

The endpoint for stopping a running chart and removing all of its layers.

#### Request body

```proto
message ChartRef {
  string name = 1;
  string namespace = 2;
  string maintainer = 3;
  string schemaVersion = 4;
}
```

#### Response - 0 OK

```proto
message Empty {}
```
