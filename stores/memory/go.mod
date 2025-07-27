module github.com/osiloke/gostore/stores/memory

go 1.23.0

toolchain go1.24.0

require (
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
)

require (
	dario.cat/mergo v1.0.0
	github.com/osiloke/gostore/common v0.0.0
)

replace github.com/osiloke/gostore/common => ../../common
