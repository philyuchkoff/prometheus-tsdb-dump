package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"

	gokitlog "github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/ryotarai/prometheus-tsdb-dump/pkg/writer"
)

func main() {
	blockPath := flag.String("block", "", "Path to block directory")
	labelKey := flag.String("label-key", "", "Filter series by this label key")
	labelValue := flag.String("label-value", "", "Filter series by this label value")
	externalLabels := flag.String("external-labels", "{}", "Labels to be added to dumped result in JSON")
	minTimestamp := flag.Int64("min-timestamp", 0, "min of timestamp of datapoints to be dumped; unix time in msec")
	maxTimestamp := flag.Int64("max-timestamp", math.MaxInt64, "max of timestamp of datapoints to be dumped; unix time in msec")
	format := flag.String("format", "victoriametrics", "Output format")
	flag.Parse()

	if *blockPath == "" {
		log.Fatal("-block argument is required")
	}

	if err := run(*blockPath, *labelKey, *labelValue, *format, *minTimestamp, *maxTimestamp, *externalLabels); err != nil {
		log.Fatalf("error: %s", err)
	}
}

func run(blockPath string, labelKey string, labelValue string, outFormat string, minTimestamp int64, maxTimestamp int64, externalLabelsJSON string) error {
	externalLabelsMap := map[string]string{}
	if err := json.NewDecoder(strings.NewReader(externalLabelsJSON)).Decode(&externalLabelsMap); err != nil {
		return fmt.Errorf("decode external labels: %w", err)
	}
	var externalLabels labels.Labels
	for k, v := range externalLabelsMap {
		externalLabels = append(externalLabels, labels.Label{Name: k, Value: v})
	}
	sort.Slice(externalLabels, func(i, j int) bool {
		return externalLabels[i].Name < externalLabels[j].Name
	})

	wr, err := writer.NewWriter(outFormat)
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	logger := gokitlog.NewLogfmtLogger(os.Stderr)

	block, err := tsdb.OpenBlock(logger, blockPath, chunkenc.NewPool())
	if err != nil {
		return fmt.Errorf("tsdb.OpenBlock: %w", err)
	}

	indexr, err := block.Index()
	if err != nil {
		return fmt.Errorf("block.Index: %w", err)
	}
	defer indexr.Close()

	chunkr, err := block.Chunks()
	if err != nil {
		return fmt.Errorf("block.Chunks: %w", err)
	}
	defer chunkr.Close()

	postings, err := indexr.Postings(labelKey, labelValue)
	if err != nil {
		return fmt.Errorf("indexr.Postings: %w", err)
	}

	var it chunkenc.Iterator
	for postings.Next() {
		ref := postings.At()
		lset := labels.Labels{}
		chks := []chunks.Meta{}
		if err := indexr.Series(ref, &lset, &chks); err != nil {
			return fmt.Errorf("indexr.Series: %w", err)
		}
		if len(externalLabels) > 0 {
			lset = append(lset, externalLabels...)
		}

		for _, meta := range chks {
			chunk, err := chunkr.Chunk(meta.Ref)
			if err != nil {
				return fmt.Errorf("chunkr.Chunk: %w", err)
			}

			var timestamps []int64
			var values []float64

			it = chunk.Iterator(it)
			for it.Next() {
				t, v := it.At()
				if math.IsNaN(v) {
					continue
				}
				if math.IsInf(v, -1) || math.IsInf(v, 1) {
					continue
				}
				if t < minTimestamp || maxTimestamp < t {
					continue
				}
				timestamps = append(timestamps, t)
				values = append(values, v)
			}
			if it.Err() != nil {
				return fmt.Errorf("iterator: %w", it.Err())
			}

			if len(timestamps) == 0 {
				continue
			}

			if err := wr.Write(&lset, timestamps, values); err != nil {
				return fmt.Errorf("Writer.Write(%v, %v, %v): %w", lset, timestamps, values, err)
			}
		}
	}

	if postings.Err() != nil {
		return fmt.Errorf("postings: %w", postings.Err())
	}

	return nil
}
