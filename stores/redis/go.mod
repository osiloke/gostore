module github.com/osiloke/gostore/stores/redis

go 1.25.0

require (
	dario.cat/mergo v1.0.2
	github.com/alicebob/miniredis/v2 v2.31.0
	github.com/osiloke/gostore/common v1.3.2
	github.com/osiloke/gostore/testing v0.0.0
	github.com/redis/go-redis/v9 v9.7.0
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rs/zerolog v1.35.1 // indirect
	github.com/yuin/gopher-lua v1.1.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/osiloke/gostore/common => ../../common
	github.com/osiloke/gostore/testing => ../../testing
)
