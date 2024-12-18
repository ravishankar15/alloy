// This service provides debug stream APIs for components.
package livedebugging

import (
	"context"

	"github.com/grafana/alloy/internal/featuregate"
	"github.com/grafana/alloy/internal/service"
)

// ServiceName defines the name used for the livedebugging service.
const ServiceName = "livedebugging"

type Service struct {
	liveDebugging *liveDebugging
}

var _ service.Service = (*Service)(nil)

func New() *Service {
	return &Service{
		liveDebugging: NewLiveDebugging(),
	}
}

type Arguments struct {
	Enabled          bool `alloy:"enabled,attr,optional"`
	BufferStreamSize int  `alloy:"buffer_stream_size,attr,optional"`
}

// SetToDefault implements syntax.Defaulter.
func (args *Arguments) SetToDefault() {
	*args = Arguments{
		Enabled:          false,
		BufferStreamSize: 1000,
	}
}

// Data implements service.Service.
// It returns the liveDebugging for the components to stream.
func (s *Service) Data() any {
	return s.liveDebugging
}

// Definition implements service.Service.
func (*Service) Definition() service.Definition {
	return service.Definition{
		Name:       ServiceName,
		ConfigType: Arguments{},
		DependsOn:  []string{},
		Stability:  featuregate.StabilityGenerallyAvailable,
	}
}

// Run implements service.Service.
func (s *Service) Run(ctx context.Context, host service.Host) error {
	s.liveDebugging.SetServiceHost(host)
	<-ctx.Done()
	return nil
}

// Update implements service.Service.
func (s *Service) Update(args any) error {
	newArgs := args.(Arguments)
	s.liveDebugging.SetEnabled(newArgs.Enabled)
	s.liveDebugging.SetBufferStreamSize(newArgs.BufferStreamSize)
	return nil
}
