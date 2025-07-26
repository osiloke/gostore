module github.com/osiloke/gostore/stores/progressive

go 1.23.0

toolchain go1.24.0

require (
	github.com/osiloke/gostore/common v0.0.0
	github.com/osiloke/gostore/mocks v0.0.0
	github.com/osiloke/gostore/worker v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.10.0
	go.uber.org/mock v0.4.0
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/rs/zerolog v1.33.0 // indirect
	golang.org/x/sys v0.27.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/osiloke/gostore/common => ../../common

replace github.com/osiloke/gostore/mocks => ../../mocks

replace github.com/osiloke/gostore/worker => ../../worker
