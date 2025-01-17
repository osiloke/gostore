VERSION := 1.1.4
NAME := $(shell echo $${PWD\#\#*/})
TARGET := ./docker/$(NAME)
all: clean build image
$(TARGET): 
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-s -w" -ldflags="-X main.VERSION=$(VERSION) -X main.BUILD=$(shell git describe --always --long --dirty)" -o $(TARGET)
build: $(TARGET)
		@true
image:
	@docker build -t $(NAME):$(VERSION) ./docker
tag: 
	@docker tag $(NAME):$(VERSION) docker.registry/$(NAME):$(VERSION)
push: 
	@docker push docker.registry/$(NAME):$(VERSION)
ktag: 
	@docker tag $(NAME):$(VERSION) gcr.io/dostow-api/$(NAME):$(VERSION)  
kpush:
	@docker push gcr.io/dostow-api/$(NAME):$(VERSION)  
clean:
	@rm -f $(TARGET)