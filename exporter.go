package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/getlantern/zenodb/core"
	"github.com/getlantern/zenodb/rpc"
)

type metricType string

const (
	counter metricType = "counter"
	gauge   metricType = "gauge"
)

type Metric struct {
	// The metric name. Note that more than one values can be mapped to
	// the same metric, as long as there have different ExtraLabels.
	Name string
	Help string
	// The type of the metric, counter/gauge/summary etc
	Type metricType
	// Extra labels applied to this particular metric in addition to
	// those mapped from dimentions.
	ExtraLabels map[string]string
}

type Job struct {
	Query string
	// Ignore these dimensions from the query result
	IgnoreDims []string
	// RenameDims maps the ZenoDB dimensions to Prometheus labels. Other
	// dimensions except those IgnoreDims are mapped to labels as they are.
	RenameDims map[string]string
	// Metrics maps the ZenoDB values to Prometheus metrics
	Metrics map[string]*Metric
}

type Config struct {
	Jobs map[string]Job
}

func createClient(addr, password string) (rpc.Client, error) {
	host, _, _ := net.SplitHostPort(addr)
	tlsConfig := &tls.Config{
		ServerName:         host,
		ClientSessionCache: tls.NewLRUClientSessionCache(100),
	}

	return rpc.Dial(addr, &rpc.ClientOpts{
		Password: password,
		Dialer: func(addr string, timeout time.Duration) (net.Conn, error) {
			conn, err := net.DialTimeout("tcp", addr, timeout)
			if err != nil {
				return nil, err
			}
			tlsConn := tls.Client(conn, tlsConfig)
			return tlsConn, tlsConn.Handshake()
		},
	})
}

func runJob(ctx context.Context, client rpc.Client, job Job, out io.Writer) error {
	if job.RenameDims == nil {
		job.RenameDims = make(map[string]string)
	}
	for _, dim := range job.IgnoreDims {
		job.RenameDims[dim] = ""
	}
	md, iterate, err := client.Query(ctx, job.Query, false /*fresh*/)
	if err != nil {
		return err
	}
	_, err = iterate(func(row *core.FlatRow) (bool, error) {
		labels := make(map[string]string)
		for dim, value := range row.Key.AsMap() {
			vs := fmt.Sprintf("%v", value)
			renamed, exists := job.RenameDims[dim]
			if exists {
				if renamed != "" {
					labels[renamed] = vs
				}
			} else {
				labels[dim] = vs
			}
		}
		idxToMetric := make(map[int]*Metric)
		for idx, name := range md.FieldNames {
			if metric, exists := job.Metrics[name]; exists {
				idxToMetric[idx] = metric
			}
		}

		for i, v := range row.Values {
			if metric, exists := idxToMetric[i]; exists {
				writeMetric(out, float64(v), row.TS, metric, labels)
			}
		}
		return true, nil
	})
	return nil
}

func writeMetric(out io.Writer, metric float64, timpstampMs int64, meta *Metric, labels map[string]string) {
	hasLabel := len(labels) > 0 || len(meta.ExtraLabels) > 0
	fmt.Fprintf(out, "# HELP %s %s\n", meta.Name, meta.Help)
	fmt.Fprintf(out, "# TYPE %s %s\n", meta.Name, meta.Type)
	fmt.Fprint(out, meta.Name)
	if hasLabel {
		io.WriteString(out, "{")
		comma := false
		writeLabel := func(name, value string) {
			if comma {
				io.WriteString(out, ",")
			} else {
				comma = true
			}
			fmt.Fprintf(out, "%s=\"%s\"", name, value)
		}
		for name, value := range labels {
			writeLabel(name, value)
		}
		for name, value := range meta.ExtraLabels {
			writeLabel(name, value)
		}
		io.WriteString(out, "}")
	}
	fmt.Fprintf(out, " %f %d\n", metric, timpstampMs)
}
