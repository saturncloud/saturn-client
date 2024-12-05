.PHONY: conda-update
conda-update:
	conda env create -n saturn-client --file environment.yaml --yes

.PHONY: format
format:
	black -l 100 saturn_client/

.PHONY: lint
lint:
	flake8 .
	black --check --diff saturn_client/
	
