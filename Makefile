clean:
	find . -type f -name "*.pyc" -delete

lint: clean
	pylint -E *.py
	cd src; pylint -E *.py
	cd test; pylint -E *.py
