package builder

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"testing"
	"time"

	"github.com/krateo-platformops/unstructured-runtime/pkg/controller/event"
	"github.com/krateo-platformops/unstructured-runtime/pkg/metrics/server"
	"github.com/krateo-platformops/unstructured-runtime/pkg/pluralizer"
	"github.com/krateo-platformops/unstructured-runtime/pkg/workqueue"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions()

	// Basic sanity checks for defaults
	require.Equal(t, 3*time.Minute, o.resyncInterval)
	require.NotNil(t, o.logger)
	require.NotNil(t, o.pluralizer)
	require.Equal(t, 5, o.maxRetries)
	require.Equal(t, "", o.namespace)
	require.NotNil(t, o.globalRateLimiter)
	require.Equal(t, "0", o.metrics.BindAddress)
}

func TestOptionSetters(t *testing.T) {
	o := defaultOptions()

	// WithPluralizer
	p := pluralizer.New()
	WithPluralizer(p)(&o)
	require.Same(t, p, o.pluralizer)

	// WithNamespace
	WithNamespace("myns")(&o)
	require.Equal(t, "myns", o.namespace)

	// WithResyncInterval
	WithResyncInterval(42 * time.Second)(&o)
	require.Equal(t, 42*time.Second, o.resyncInterval)

	// WithMaxRetries
	WithMaxRetries(10)(&o)
	require.Equal(t, 10, o.maxRetries)

	so := server.Options{BindAddress: ":9090"}
	// WithMetrics
	WithMetrics(so)(&o)
	require.Equal(t, so, o.metrics)

	// WithWatchAnnotations with a map
	anns := event.NewAnnotationEvents(event.AnnotationEvent{
		EventType:  event.Delete,
		OnAction:   event.OnAny,
		Annotation: "my-annotation",
	})
	WithWatchAnnotations(anns...)(&o)
	require.True(t, o.watchAnnotations.HasAnnotation("my-annotation"))

	WithActionEvent(event.CRCreated, event.Update)(&o)
	require.Equal(t, event.Update, o.actionsEvent.GetEventType(event.CRCreated))

	// WithTracerProvider; unset means the process default.
	require.Nil(t, defaultOptions().tracerProvider)
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	WithTracerProvider(tp)(&o)
	require.Same(t, tp, o.tracerProvider)
}

func TestConfigurationValidate(t *testing.T) {
	// 1) missing Config
	c1 := Configuration{
		Config:       nil,
		GVR:          schema.GroupVersionResource{Group: "g", Version: "v", Resource: "r"},
		ProviderName: "p",
	}
	require.Error(t, c1.validate())

	// 2) missing GVR fields
	c2 := Configuration{
		Config:       &rest.Config{},
		GVR:          schema.GroupVersionResource{},
		ProviderName: "p",
	}
	require.Error(t, c2.validate())

	// 3) missing ProviderName
	c3 := Configuration{
		Config:       &rest.Config{},
		GVR:          schema.GroupVersionResource{Group: "g", Version: "v", Resource: "r"},
		ProviderName: "",
	}
	require.Error(t, c3.validate())

	// 4) valid configuration
	c4 := Configuration{
		Config:       &rest.Config{},
		GVR:          schema.GroupVersionResource{Group: "g", Version: "v", Resource: "r"},
		ProviderName: "p",
	}
	require.NoError(t, c4.validate())
}

func TestNew_ReturnsError_OnInvalidConfiguration(t *testing.T) {
	// Passing an invalid configuration (nil Config) should return an error early.
	_, err := Build(context.Background(), Configuration{
		Config:       nil,
		GVR:          schema.GroupVersionResource{Group: "g", Version: "v", Resource: "r"},
		ProviderName: "p",
	})
	require.Error(t, err)
}

func TestNew_ReturnsController(t *testing.T) {
	// Passing a valid configuration should return a controller instance.
	c, err := Build(context.Background(), Configuration{
		Config:       &rest.Config{},
		GVR:          schema.GroupVersionResource{Group: "g", Version: "v", Resource: "r"},
		ProviderName: "p",
	},
		WithNamespace("myns"),
		WithResyncInterval(42*time.Second),
		WithMaxRetries(10),
		WithMetrics(server.Options{BindAddress: ":9090"}),
		WithPluralizer(pluralizer.New()),
		WithGlobalRateLimiter(workqueue.NewExponentialTimedFailureRateLimiter[any](2*time.Second, 1*time.Minute)),
	)
	require.NoError(t, err)
	require.NotNil(t, c)
}
