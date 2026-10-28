.PHONY: install test test-fast up down generate

install:
	pip install -r requirements-dev.txt

test:            ## everything, needs Java 17
	pytest -v

test-fast:       ## pure-Python rules/contract/generator tests, no JVM
	pytest -v -m "not spark"

up:
	docker compose up -d --build

down:
	docker compose down -v

generate:        ## print 10 sample events
	PYTHONPATH=src python -m txnstream.generator --count 10 --seed 42
