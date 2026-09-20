package frankencache

import (
	"encoding/json"
	"errors"

	"github.com/b1tc0re/frankenphp-tiered-cache/internal/observability"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
)

// FrankenCacheMetrics registers the cache collector in the registry belonging
// to the current Caddy context. The module intentionally owns no registry and
// no collector beyond that context lifetime.
type FrankenCacheMetrics struct{}

func init() {
	caddy.RegisterModule(FrankenCacheMetrics{})
	httpcaddyfile.RegisterGlobalOption("franken_cache_metrics", parseFrankenCacheMetrics)
}

func (FrankenCacheMetrics) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "franken_cache.metrics",
		New: func() caddy.Module {
			return new(FrankenCacheMetrics)
		},
	}
}

func (m *FrankenCacheMetrics) Provision(ctx caddy.Context) error {
	if phpTieredMetrics == nil {
		return errors.New("franken_cache: metrics state is not initialized")
	}
	registry := ctx.GetMetricsRegistry()
	if registry == nil {
		return errors.New("franken_cache: Caddy metrics registry is not available")
	}
	if phpTieredL1Stats != nil {
		return registry.Register(observability.NewCollectorWithL1(phpTieredMetrics, extensionVersion(), phpTieredL1Stats))
	}
	return registry.Register(observability.NewCollector(phpTieredMetrics, extensionVersion()))
}

func (*FrankenCacheMetrics) Start() error { return nil }

func (*FrankenCacheMetrics) Stop() error { return nil }

func parseFrankenCacheMetrics(d *caddyfile.Dispenser, existingVal any) (any, error) {
	if existingVal != nil {
		return nil, d.Errf("franken_cache_metrics may only be configured once")
	}
	d.Next()
	if d.NextArg() || d.NextBlock(0) {
		return nil, d.ArgErr()
	}
	return httpcaddyfile.App{
		Name:  "franken_cache.metrics",
		Value: json.RawMessage(`{}`),
	}, nil
}

var (
	_ caddy.Module      = FrankenCacheMetrics{}
	_ caddy.App         = (*FrankenCacheMetrics)(nil)
	_ caddy.Provisioner = (*FrankenCacheMetrics)(nil)
)
