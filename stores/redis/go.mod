module github.com/osiloke/gostore/stores/redis

go 1.25.0

require (
	github.com/alicebob/miniredis/v2 v2.31.0
	github.com/osiloke/gostore/common v0.0.0
	github.com/osiloke/gostore/testing v0.0.0
	github.com/redis/go-redis/v9 v9.7.0
	github.com/stretchr/testify v1.10.0
	dario.cat/mergo v1.0.2
)

replace (
	github.com/osiloke/gostore/common => ../../common
	github.com/osiloke/gostore/testing => ../../testing
)
