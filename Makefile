install:
	pip install -r requirements.txt -I

lint:
	black pyspark_demo tests -l 120 --target-version=py37

test:
	black pyspark_demo pyspark_demo -l 120 --target-version=py37 --check
	pytest -s -v --cov=pyspark_demo tests --cov-fail-under=65 --disable-pytest-warnings
