package redis

/*
MIT License

Copyright (c) 2026 Shane

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import (
	"crypto/tls"
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	t.Parallel()
	c := defaults()
	if c.mode != modeStandalone {
		t.Errorf("mode = %v, want standalone", c.mode)
	}
	if len(c.addrs) != 1 || c.addrs[0] != defaultAddr {
		t.Errorf("addrs = %v, want [%s]", c.addrs, defaultAddr)
	}
	if c.poolSize < 10 {
		t.Errorf("poolSize = %d, want >= 10", c.poolSize)
	}
	if c.dialTimeout != defaultDialTimeout || c.readTimeout != defaultReadTimeout || c.writeTimeout != defaultWriteTimeout {
		t.Errorf("timeouts = %v/%v/%v, want defaults", c.dialTimeout, c.readTimeout, c.writeTimeout)
	}
	if c.maxRetries != defaultMaxRetries {
		t.Errorf("maxRetries = %d, want %d", c.maxRetries, defaultMaxRetries)
	}
	if c.minRetryBackoff != defaultMinRetryBackoff || c.maxRetryBackoff != defaultMaxRetryBackoff {
		t.Errorf("retry backoff = %v/%v, want defaults", c.minRetryBackoff, c.maxRetryBackoff)
	}
	if c.logger != nil || c.observer != nil {
		t.Error("logger/observer should default to nil so the hook is not installed")
	}
}

// apply builds a config from defaults and the given options, as Connect does.
func apply(opts ...Option) config {
	c := defaults()
	for _, o := range opts {
		o(&c)
	}
	return c
}

func TestModeSelection(t *testing.T) {
	t.Parallel()
	if c := apply(WithAddr("host:6379")); c.mode != modeStandalone || c.addrs[0] != "host:6379" {
		t.Errorf("WithAddr: mode=%v addrs=%v", c.mode, c.addrs)
	}
	if c := apply(WithSentinel("mymaster", "s1:26379", "s2:26379")); c.mode != modeSentinel ||
		c.masterName != "mymaster" || len(c.addrs) != 2 {
		t.Errorf("WithSentinel: mode=%v master=%q addrs=%v", c.mode, c.masterName, c.addrs)
	}
	if c := apply(WithCluster("n1:6379", "n2:6379")); c.mode != modeCluster || len(c.addrs) != 2 {
		t.Errorf("WithCluster: mode=%v addrs=%v", c.mode, c.addrs)
	}
	// Last mode option wins.
	if c := apply(WithCluster("n1:6379"), WithAddr("host:6379")); c.mode != modeStandalone {
		t.Errorf("last option should win: mode=%v", c.mode)
	}
}

func TestScalarOptions(t *testing.T) {
	t.Parallel()
	tlsCfg := &tls.Config{ServerName: "redis.example.com"} //nolint:gosec // test-only config, no MinVersion needed
	c := apply(
		WithPassword("s3cret"),
		WithDB(4),
		WithTLS(tlsCfg),
		WithPool(50),
		WithTimeouts(9*time.Second, 8*time.Second, 7*time.Second),
		WithRetry(6, 20*time.Millisecond, time.Second),
	)
	if c.password != "s3cret" || c.db != 4 || c.tlsConfig != tlsCfg || c.poolSize != 50 {
		t.Errorf("scalar options not applied: %+v", c)
	}
	if c.dialTimeout != 9*time.Second || c.readTimeout != 8*time.Second || c.writeTimeout != 7*time.Second {
		t.Errorf("timeouts not applied: %v/%v/%v", c.dialTimeout, c.readTimeout, c.writeTimeout)
	}
	if c.maxRetries != 6 || c.minRetryBackoff != 20*time.Millisecond || c.maxRetryBackoff != time.Second {
		t.Errorf("retry not applied: %d/%v/%v", c.maxRetries, c.minRetryBackoff, c.maxRetryBackoff)
	}
}

func TestNonPositiveValuesKeepDefaults(t *testing.T) {
	t.Parallel()
	c := apply(
		WithPool(0),
		WithTimeouts(0, -1, 0),
		WithRetry(-1, 0, 0),
	)
	d := defaults()
	if c.poolSize != d.poolSize {
		t.Errorf("WithPool(0) changed poolSize to %d", c.poolSize)
	}
	if c.dialTimeout != d.dialTimeout || c.readTimeout != d.readTimeout || c.writeTimeout != d.writeTimeout {
		t.Error("non-positive timeouts should keep defaults")
	}
	if c.maxRetries != d.maxRetries || c.minRetryBackoff != d.minRetryBackoff {
		t.Error("negative attempts / zero backoff should keep defaults")
	}
}

func TestRetryZeroDisables(t *testing.T) {
	t.Parallel()
	if c := apply(WithRetry(0, 0, 0)); c.maxRetries != 0 {
		t.Errorf("WithRetry(0,...) should disable retries, got maxRetries=%d", c.maxRetries)
	}
}

func TestOptionsMapping(t *testing.T) {
	t.Parallel()
	c := apply(
		WithAddr("host:6379"),
		WithPassword("pw"),
		WithDB(2),
		WithPool(33),
		WithTimeouts(4*time.Second, 3*time.Second, 2*time.Second),
		WithRetry(5, 10*time.Millisecond, 400*time.Millisecond),
	)
	so := c.standaloneOptions()
	if so.Addr != "host:6379" || so.Password != "pw" || so.DB != 2 || so.PoolSize != 33 {
		t.Errorf("standaloneOptions basics wrong: %+v", so)
	}
	if so.MaxRetries != 5 || so.MinRetryBackoff != 10*time.Millisecond || so.MaxRetryBackoff != 400*time.Millisecond {
		t.Errorf("standaloneOptions retry wrong: %+v", so)
	}

	fc := apply(WithSentinel("mymaster", "s1:26379", "s2:26379"), WithPassword("pw"), WithDB(1))
	fo := fc.failoverOptions()
	if fo.MasterName != "mymaster" || len(fo.SentinelAddrs) != 2 || fo.Password != "pw" || fo.DB != 1 {
		t.Errorf("failoverOptions wrong: %+v", fo)
	}

	cc := apply(WithCluster("n1:6379", "n2:6379"), WithPool(12))
	co := cc.clusterOptions()
	if len(co.Addrs) != 2 || co.PoolSize != 12 {
		t.Errorf("clusterOptions wrong: %+v", co)
	}
}
