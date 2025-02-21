VERSION := 1.0.0
NAME := gostore-$(shell echo $${PWD\#\#*/})
TARGET := ./docker/$(NAME)

all: clean build image

$(TARGET):
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -ldflags="-X main.VERSION=$(VERSION) -X main.BUILD=$(shell git describe --always --long --dirty)" -o $(TARGET)

build: $(TARGET)
	@true

image:
	@docker build -t $(NAME):$(VERSION) ./docker

# Version Bumping
bump_patch:
	@VERSION=$(shell echo $(VERSION) | awk -F. '{print $$1"."$$2"."($$3+1)}'); echo "VERSION=${VERSION}" >> Makefile

# Tagging and Pushing
tag:
	@docker tag $(NAME):$(VERSION) docker.registry/$(NAME):$(VERSION)
push:
	@docker push docker.registry/$(NAME):$(VERSION)
ktag:
	@docker tag $(NAME):$(VERSION) gcr.io/dostow-api/$(NAME):$(VERSION)
kpush:
	@docker push gcr.io/dostow-api/$(NAME):$(VERSION)

release: bump_patch tag push ktag kpush
	@git add Makefile
	@git commit -m "Release $(VERSION)"
	@git tag v$(VERSION)
	@git push origin v$(VERSION)
	@git push origin main

clean:
	@rm -f $(TARGET)