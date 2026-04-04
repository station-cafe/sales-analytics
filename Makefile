.PHONY: fetch analyze render all clean

VENV = .venv/bin/python3

fetch:
	$(VENV) fetch_data.py

analyze:
	$(VENV) analyze.py

render:
	$(VENV) render.py

all: fetch analyze render

clean:
	rm -rf data/*.json analysis.json output/index.html
