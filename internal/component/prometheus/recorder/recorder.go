package recorder

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/grafana/alloy/internal/component"
	"github.com/grafana/alloy/internal/component/prometheus"
	"github.com/grafana/alloy/internal/featuregate"
	"github.com/grafana/alloy/internal/service/labelstore"
	"github.com/grafana/alloy/internal/service/livedebugging"
	prometheus_client "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

const name = "prometheus.recorder"

func init() {
	component.Register(component.Registration{
		Name:      name,
		Stability: featuregate.StabilityGenerallyAvailable,
		Args:      Arguments{},
		Exports:   Exports{},
		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

type Arguments struct {
	ForwardTo []storage.Appendable `alloy:"forward_to,attr"`
	Rule      string               `alloy:"rule,attr"`
}

type Exports struct {
	Receiver storage.Appendable `alloy:"receiver,attr"`
}

type Component struct {
	mut              sync.RWMutex
	opts             component.Options
	receiver         *prometheus.Interceptor
	metricsProcessed prometheus_client.Counter
	fanout           *prometheus.Fanout
	exited           atomic.Bool

	debugDataPublisher livedebugging.DebugDataPublisher
}

var (
	_ component.Component     = (*Component)(nil)
	_ component.LiveDebugging = (*Component)(nil)
)

func New(o component.Options, args Arguments) (*Component, error) {
	liveDebugging, err := o.GetServiceData(livedebugging.ServiceName)
	if err != nil {
		return nil, err
	}

	data, err := o.GetServiceData(labelstore.ServiceName)
	if err != nil {
		return nil, err
	}
	ls := data.(labelstore.LabelStore)
	c := &Component{
		opts:               o,
		debugDataPublisher: liveDebugging.(livedebugging.DebugDataPublisher),
	}
	c.metricsProcessed = prometheus_client.NewCounter(prometheus_client.CounterOpts{
		Name: "alloy_prometheus_recorder_metrics_processed",
		Help: "Total number of metrics processed",
	})

	err = o.Registerer.Register(c.metricsProcessed)
	if err != nil {
		return nil, err
	}

	c.fanout = prometheus.NewFanout(args.ForwardTo, o.ID, o.Registerer, ls, prometheus.NoopMetadataStore{})
	c.receiver = prometheus.NewInterceptor(
		c.fanout,
		ls,
		prometheus.WithAppendHook(func(ref storage.SeriesRef, l labels.Labels, t int64, v float64, next storage.Appender) (storage.SeriesRef, error) {
			if c.exited.Load() {
				return 0, fmt.Errorf("%s has exited", o.ID)
			}

			fmt.Printf("Processing the append data %s\n", l.String())

			return next.Append(0, l, t, v)
		}),
		prometheus.WithExemplarHook(func(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar, next storage.Appender) (storage.SeriesRef, error) {
			if c.exited.Load() {
				return 0, fmt.Errorf("%s has exited", o.ID)
			}

			fmt.Printf("Processing the exemplar data %s\n", l.String())

			// Since SeriesRefs are tied to the labels, we send zero to indicate the seriesRef should be recalculated downstream.
			return next.AppendExemplar(0, l, e)
		}),
		prometheus.WithMetadataHook(func(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata, next storage.Appender) (storage.SeriesRef, error) {
			if c.exited.Load() {
				return 0, fmt.Errorf("%s has exited", o.ID)
			}

			fmt.Printf("Processing the metadata data %s\n", l.String())

			// Since SeriesRefs are tied to the labels, we send zero to indicate the seriesRef should be recalculated downstream.
			return next.UpdateMetadata(0, l, m)
		}),
		prometheus.WithHistogramHook(func(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram, next storage.Appender) (storage.SeriesRef, error) {
			if c.exited.Load() {
				return 0, fmt.Errorf("%s has exited", o.ID)
			}

			fmt.Printf("Processing the histogram data %s\n", l.String())

			// Since SeriesRefs are tied to the labels, we send zero to indicate the seriesRef should be recalculated downstream.
			return next.AppendHistogram(0, l, t, h, fh)
		}),
	)

	o.OnStateChange(Exports{Receiver: c.receiver})

	if err = c.Update(args); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Component) Run(ctx context.Context) error {
	defer c.exited.Store(true)

	<-ctx.Done()
	return nil
}

func (c *Component) Update(args component.Arguments) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	newArgs := args.(Arguments)
	c.fanout.UpdateChildren(newArgs.ForwardTo)

	c.opts.OnStateChange(Exports{Receiver: c.receiver})

	return nil
}

func (c *Component) LiveDebugging() {}
