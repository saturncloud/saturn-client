.PHONY: conda-update
conda-update:
	conda env update -n saturn-client --file environment.yaml

.PHONY: format
format:
	black saturn_client/

.PHONY: lint
lint:
	flake8 .
	black --check --diff saturn_client/
	pylint saturn_client/
