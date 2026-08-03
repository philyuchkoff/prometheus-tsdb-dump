package writer

import (
	"encoding/json"
	"os"

	"github.com/prometheus/prometheus/model/labels"
)

type VictoriaMetricsWriter struct {
	enc *json.Encoder
}

func NewVictoriaMetricsWriter() (*VictoriaMetricsWriter, error) {
	return &VictoriaMetricsWriter{enc: json.NewEncoder(os.Stdout)}, nil
}

type victoriaMetricsLine struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

func (w *VictoriaMetricsWriter) Write(lset labels.Labels, timestamps []int64, values []float64) error {
	metric := map[string]string{}
	lset.Range(func(l labels.Label) {
		metric[l.Name] = l.Value
	})

	err := w.enc.Encode(victoriaMetricsLine{
		Metric:     metric,
		Values:     values,
		Timestamps: timestamps,
	})
	if err != nil {
		return err
	}
	return nil
}
