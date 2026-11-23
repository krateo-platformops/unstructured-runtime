package builder

import (
	"context"
	"reflect"
	"testing"

	"github.com/krateo-platformops/unstructured-runtime/pkg/controller"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

type nopCredentialUse struct{ controller.CredentialUse }

// WithCredentialUse reaches the built controller. CONTROL: without it there is none.
func TestWithCredentialUse(t *testing.T) {
	has := func(opts ...FuncOption) bool {
		c, err := Build(context.Background(), Configuration{
			Config:       &rest.Config{},
			GVR:          schema.GroupVersionResource{Group: "g", Version: "v", Resource: "r"},
			ProviderName: "p",
		}, opts...)
		require.NoError(t, err)
		f := reflect.ValueOf(c).Elem().FieldByName("credentialUse")
		require.True(t, f.IsValid(), "the Controller has no such field: this test lost its subject")
		return !f.IsNil()
	}
	require.True(t, has(WithCredentialUse(nopCredentialUse{})))
	require.False(t, has())
}
