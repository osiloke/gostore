module github.com/osiloke/gostore/stores/memory

go 1.25.0

require (
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/rs/zerolog v1.35.1 // indirect
	golang.org/x/sys v0.45.0 // indirect
)

require (
	dario.cat/mergo v1.0.2
	github.com/osiloke/gostore/common v0.0.0
)

replace github.com/osiloke/gostore/common => ../../common
