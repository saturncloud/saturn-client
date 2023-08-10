.PHONY: conda-update
conda-update:
	mamba env create -n saturn-client --file environment.yaml --force

.PHONY: format
format:
	black saturn_client/

.PHONY: lint
lint:
	flake8 .
	black --check --diff saturn_client/
	pylint saturn_client/
