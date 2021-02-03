venv:
	python3.7 -m venv venv

.PHONY: venv
install: activate
	./venv/bin/pip install -r requirements.txt
	