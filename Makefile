SHELL := /bin/bash
SHA=$(shell git rev-parse HEAD)
DATE=$(shell date +%y.%m.%d)
export TAG?=${SHA}

 .PHONY: build
build:
	$(info Make: Building docker images: TAG=${SHA})
	@docker-compose build

 .PHONY: clean
clean:
	$(info Make: Clean docker containers)
	@docker-compose down -v --remove-orphans

 .PHONY: run
run:
	$(info Make: Bring local docker cluster up)
	@docker-compose up

 .PHONY: test-helm
test-helm:
	$(info Make: Run helm lint TAG=${SHA})
	@helm lint chart/${CHART_NAME}

 .PHONY: publish
publish:
	$(info Make: Build & Publish docker images: TAG=${SHA})
	@docker-compose push health-hq health-wagon
