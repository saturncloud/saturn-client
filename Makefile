.PHONY: conda-update
conda-update:
	conda env update -n saturn-client --file environment.yaml

.PHONY: format
format:
	black .

.PHONY: lint
lint:
	flake8 .
	black --check --diff .
	pylint saturn_client/
