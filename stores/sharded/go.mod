module github.com/osiloke/gostore/stores/sharded

go 1.25.0

replace github.com/osiloke/gostore/common => ../../common

replace github.com/osiloke/gostore/pool => ../../pool

replace github.com/osiloke/gostore/mocks => ../../mocks

replace github.com/osiloke/gostore/stores/memory => ../memory

require (
	github.com/osiloke/gostore/common v0.0.0
	github.com/osiloke/gostore/pool v0.0.0-00010101000000-000000000000
	github.com/osiloke/gostore/stores/memory v0.0.0-00010101000000-000000000000
)

require (
	dario.cat/mergo v1.0.2 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/rs/zerolog v1.35.1 // indirect
	golang.org/x/sys v0.45.0 // indirect
)
