# Prometheus TSDB dump

prometheus-tsdb-dump reads a Prometheus TSDB block and writes metrics.

## Requirements

- Go 1.25+ (uses the latest Prometheus TSDB APIs — `log/slog`, `model/labels`)
- Prometheus `tsdb` blocks (v2/v3 format)

## Installation

```
$ git clone https://github.com/philyuchkoff/prometheus-tsdb-dump
$ cd prometheus-tsdb-dump
$ make build
```

## Usage

### Dump all series in a block

```
$ prometheus-tsdb-dump -block /path/to/prometheus-data/block-ulid -format victoriametrics
{"metric":{"__name__":"up","job":"node-exporter","instance":"a"},"values":[1,1,1],"timestamps":[1578636058619,1578636118619,1578636178619]}
```

### Filter by label

```
$ prometheus-tsdb-dump -block /path/to/block-ulid -label-key __name__ -label-value up
```

### Add external labels and time range

```
$ prometheus-tsdb-dump -block /path/to/block-ulid \
  -external-labels '{"cluster":"prod","region":"eu"}' \
  -min-timestamp 1577836800000 \
  -max-timestamp 1577923200000
```

## Options

- `-block`: Path to the TSDB block directory (required)
- `-format`: Output format (default: `victoriametrics`)
- `-label-key`, `-label-value`: Only dump series having the given label key/value
- `-external-labels`: Labels to be added to every dumped series, given as a JSON object (default: `{}`)
- `-min-timestamp`: Minimum timestamp of exported samples (unix time in msec)
- `-max-timestamp`: Maximum timestamp of exported samples (unix time in msec)

## Behaviour

- Datapoints with `NaN`, `+Inf` and `-Inf` values are skipped.
- Datapoints outside the `-min-timestamp` / `-max-timestamp` range are skipped.
- Series with no remaining datapoints are not written.
- External labels are sorted by label name, so the output is deterministic.

## Output Formats

Output format can be configured via `-format` option.

### `victoriametrics`

`victoriametrics` format is a JSON streaming format which can be imported via [VictoriaMetrics' `/api/v1/import` API](https://github.com/VictoriaMetrics/VictoriaMetrics#how-to-import-time-series-data). It looks like:

```
{"metric":{"__name__":"up","job":"node-exporter"},"values":[1,1,1],"timestamps":[1578636058619,1578636118619,1578636178619]}
```

#### How to import TSDB data from Prometheus to VictoriaMetrics

First, take a snapshot of Prometheus TSDB:

```
$ curl -XPOST http://your-prometheus:9090/api/v1/admin/tsdb/snapshot
{"status":"success","data":{"name":"20200110T104512Z-xxxxxxxxxxxx"}}
```

Then, import data to VictoriaMetrics:

```
$ cd /path/to/prometheus/data/snapshots/20200110T104512Z-xxxxxxxxxxxx
$ parallelism="$(nproc)"
$ find . -mindepth 1 -maxdepth 1 -type d | xargs -n1 -P "$parallelism" sh -c 'echo $0; prometheus-tsdb-dump-linux -block "$0" -format victoriametrics | curl http://your-victoriametrics:8428/api/v1/import -T -'
```
