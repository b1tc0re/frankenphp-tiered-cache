IMAGE ?= frankenphp-tiered-cache:dev

.PHONY: build smoke

build:
	docker build -t $(IMAGE) .

smoke:
	docker build --progress=plain -t $(IMAGE) .
