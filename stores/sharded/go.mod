module github.com/gostore/gostore/stores/sharded

go 1.24.0

replace github.com/gostore/gostore/common => ../../common

replace github.com/gostore/gostore/pool => ../../pool

replace github.com/gostore/gostore/mocks => ../../mocks

replace github.com/gostore/gostore/stores/memory => ../memory

require (
	github.com/gostore/gostore/common v0.0.0
	github.com/gostore/gostore/pool v0.0.0-00010101000000-000000000000
	github.com/gostore/gostore/stores/memory v0.0.0-00010101000000-000000000000
)

require (
	dario.cat/mergo v1.0.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/osiloke/gostore/common v0.0.0-20240719162349-fe2217882b55 // indirect
	github.com/rs/zerolog v1.33.0 // indirect
	golang.org/x/sys v0.27.0 // indirect
)
