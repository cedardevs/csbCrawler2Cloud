#!/bin/sh

#scl enable rh-python38 'python ~/bin/hello.py'
source /opt/rh/rh-python38/enable
pipenv run python --version
pipenv run python ~/bin/hello.py

