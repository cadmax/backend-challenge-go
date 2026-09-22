package bootstrap

import (
	"go.uber.org/fx"
	"testing"
)

func TestComposition(t *testing.T) {
	if err := fx.ValidateApp(Module()); err != nil {
		t.Fatal(err)
	}
}
