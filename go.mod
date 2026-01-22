module github.com/grafana/walqueue

go 1.24.9

toolchain go1.24.12

require (
	// This is pinned by a replace further down due to a bug
	github.com/deneonet/benc v1.1.7
	github.com/go-kit/log v0.2.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v1.0.0
	github.com/klauspost/compress v1.18.3
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.67.4
	github.com/prometheus/prometheus v0.309.1
	github.com/stretchr/testify v1.11.1
	github.com/tinylib/msgp v1.3.0
	go.uber.org/atomic v1.11.0
	golang.design/x/chann v0.1.2
)

require (
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/elastic/go-freelru v0.16.0
	github.com/prometheus/client_golang/exp v0.0.0-20251212205219-7ba246a648ca
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dennwc/varint v1.0.0 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/exp v0.0.0-20250808145144-a408d31f581a // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/oauth2 v0.34.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Do not remove this until the bug breaking backwards compatibility is resolved: https://github.com/deneonet/benc/issues/13
replace github.com/deneonet/benc => github.com/deneonet/benc v1.1.7
