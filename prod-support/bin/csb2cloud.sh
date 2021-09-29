#!/bin/sh

#scl enable rh-python38 'python ~/bin/hello.py'
source ~/bin/setCsbcrawlerEnv.sh
source /opt/rh/rh-python38/enable
cd /home/csb_bdp/csb_deploy/csbCrawler2Cloud-v1.0.6
pipenv run python launch_app.py

