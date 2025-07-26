module github.com/osiloke/gostore-mocks

go 1.23.0

toolchain go1.24.0

require (
	github.com/osiloke/gostore/common v0.0.0
	go.uber.org/mock v0.4.0
)

require (
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/rs/zerolog v1.33.0 // indirect
	golang.org/x/sys v0.27.0 // indirect
)

replace github.com/osiloke/gostore/common => ../common
