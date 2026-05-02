PYTHON ?= python3

.PHONY: setup test sample up down producer stream gold health dbt-docs

setup:
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt

test:
	$(PYTHON) -m unittest discover -s tests -p "test_*.py"
	$(PYTHON) -m compileall data-generator kafka-producer data-quality spark-streaming scripts

sample:
	$(PYTHON) data-generator/generate_events.py --topic all --count 25 --out-dir sample-data/events

up:
	docker compose up -d zookeeper kafka kafka-ui

down:
	docker compose down

producer:
	$(PYTHON) kafka-producer/producer.py --bootstrap-server $${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092} --rate-per-second 5

stream:
	spark-submit spark-streaming/jobs/streaming_pipeline.py --bootstrap-servers $${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}

gold:
	spark-submit spark-streaming/jobs/gold_refresh.py --lakehouse-root lakehouse

health:
	$(PYTHON) scripts/pipeline_health.py --lakehouse-root lakehouse --checkpoint-root checkpoints

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve
