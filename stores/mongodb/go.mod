module github.com/osiloke/gostore/stores/mongodb

go 1.23.0

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/rs/zerolog v1.34.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.mongodb.org/mongo-driver v1.17.2
	golang.org/x/crypto v0.29.0 // indirect
	golang.org/x/sync v0.9.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/text v0.20.0 // indirect
)

require github.com/osiloke/gostore/common v0.0.0-20240719162349-fe2217882b55

require github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect

replace github.com/osiloke/gostore/common => ../../common
