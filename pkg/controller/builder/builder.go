package builder

import (
	"context"
	"fmt"
	"net/http"

	"github.com/krateo-platformops/plumbing/kubeutil/event"
	"github.com/krateo-platformops/plumbing/kubeutil/eventrecorder"
	"github.com/krateo-platformops/plumbing/shortid"
	"github.com/krateo-platformops/unstructured-runtime/pkg/controller"
	metricsserver "github.com/krateo-platformops/unstructured-runtime/pkg/metrics/server"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func GetConfig() (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	if cfg.QPS == 0.0 {
		// Disable client-side ratelimer by default, we can rely on
		// API priority and fairness
		cfg.QPS = -1
	}
	return cfg, nil
}

type Configuration struct {
	Config       *rest.Config
	GVR          schema.GroupVersionResource
	ProviderName string
}

func (c Configuration) validate() error {
	if c.Config == nil {
		return fmt.Errorf("kube config is required")
	}
	if c.GVR.Group == "" || c.GVR.Version == "" || c.GVR.Resource == "" {
		return fmt.Errorf("gvr is required")
	}
	if c.ProviderName == "" {
		return fmt.Errorf("provider name is required")
	}
	return nil
}

type FuncOption func(o *options)

func Build(ctx context.Context, conf Configuration, optsFuncs ...FuncOption) (*controller.Controller, error) {
	if err := conf.validate(); err != nil {
		return nil, err
	}
	opts := defaultOptions()
	for _, o := range optsFuncs {
		o(&opts)
	}

	sid, err := shortid.New(1, shortid.DefaultABC, 2342)
	if err != nil {
		opts.logger.Info("failed to create shortid", "err", err)
		return nil, fmt.Errorf("failed to create shortid: %w", err)
	}

	throttleRec, err := eventrecorder.CreateWithThrottle(ctx, conf.Config, conf.ProviderName, nil)
	if err != nil {
		opts.logger.Error(err, "failed to create event recorder")
		return nil, fmt.Errorf("failed to create event recorder: %w", err)
	}
	rec, err := eventrecorder.Create(ctx, conf.Config, conf.ProviderName, nil)
	if err != nil {
		opts.logger.Error(err, "failed to create event recorder")
		return nil, fmt.Errorf("failed to create event recorder: %w", err)
	}

	metricsServer, err := metricsserver.NewServer(opts.metrics, conf.Config, http.DefaultClient)
	if err != nil {
		opts.logger.Error(err, "failed to create metrics server")
		return nil, fmt.Errorf("failed to create metrics server: %w", err)
	}

	client, err := dynamic.NewForConfig(conf.Config)
	if err != nil {
		opts.logger.Error(err, "failed to create dynamic client")
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	ctrl, err := controller.New(sid, controller.Options{
		Pluralizer:              opts.pluralizer,
		Client:                  client,
		GVR:                     conf.GVR,
		Namespace:               opts.namespace,
		ResyncInterval:          opts.resyncInterval,
		Recorder:                event.NewAPIRecorder(rec),
		ThrottledRecorder:       event.NewAPIRecorder(throttleRec),
		Logger:                  opts.logger,
		Metrics:                 opts.metricsRecorder,
		ListWatcher:             opts.listWatcher,
		GlobalRateLimiter:       opts.globalRateLimiter,
		MetricsServer:           metricsServer,
		WatchAnnotations:        opts.watchAnnotations,
		MaxRetries:              opts.maxRetries,
		RateLimitGovernor:       opts.rateLimitGovernor,
		ActionsEvent:            opts.actionsEvent,
		GracefulShutdownTimeout: opts.gracefulShutdownTimeout,
	})
	if err != nil {
		opts.logger.Error(err, "failed to create controller")
		return nil, fmt.Errorf("failed to create controller: %w", err)
	}

	return ctrl, nil
}
