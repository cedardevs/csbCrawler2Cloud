#!/bin/bash
## clear manifest so this version of run always does the metadata and extract_csv_files work
rm -r manifestdir
pipenv run python launch_app.py
