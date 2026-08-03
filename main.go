package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

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
	externalSlice := []labels.Label{}
	for k, v := range externalLabelsMap {
		externalSlice = append(externalSlice, labels.Label{Name: k, Value: v})
	}
	sort.Slice(externalSlice, func(i, j int) bool {
		return externalSlice[i].Name < externalSlice[j].Name
	})

	wr, err := writer.NewWriter(outFormat)
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	block, err := tsdb.OpenBlock(logger, blockPath, chunkenc.NewPool(), tsdb.DefaultPostingsDecoderFactory)
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

	ctx := context.Background()

	var postings index.Postings
	if labelKey == "" {
		postings, err = allPostings(ctx, indexr)
		if err != nil {
			return err
		}
	} else {
		postings, err = indexr.Postings(ctx, labelKey, labelValue)
		if err != nil {
			return fmt.Errorf("indexr.Postings: %w", err)
		}
	}

	var builder labels.ScratchBuilder
	var it chunkenc.Iterator
	for postings.Next() {
		builder.Reset()
		chks := []chunks.Meta{}
		if err := indexr.Series(postings.At(), &builder, &chks); err != nil {
			return fmt.Errorf("indexr.Series: %w", err)
		}
		for _, l := range externalSlice {
			builder.Add(l.Name, l.Value)
		}
		lset := builder.Labels()

		for _, meta := range chks {
			chunk, iterable, err := chunkr.ChunkOrIterable(meta)
			if err != nil {
				return fmt.Errorf("chunkr.ChunkOrIterable: %w", err)
			}

			var timestamps []int64
			var values []float64

			var data chunkenc.Iterable = chunk
			if iterable != nil {
				data = iterable
			}
			it = data.Iterator(it)
			for it.Next() != chunkenc.ValNone {
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

			if err := wr.Write(lset, timestamps, values); err != nil {
				return fmt.Errorf("Writer.Write(%v, %v, %v): %w", lset, timestamps, values, err)
			}
		}
	}

	if postings.Err() != nil {
		return fmt.Errorf("postings: %w", postings.Err())
	}

	return nil
}

func allPostings(ctx context.Context, indexr tsdb.IndexReader) (index.Postings, error) {
	names, err := indexr.LabelNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("indexr.LabelNames: %w", err)
	}

	postings := make([]index.Postings, 0, len(names))
	for _, name := range names {
		postings = append(postings, indexr.PostingsForAllLabelValues(ctx, name))
	}
	return index.Merge(ctx, postings...), nil
}
