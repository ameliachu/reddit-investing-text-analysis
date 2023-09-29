#!/bin/sh

# -- Creating Folders --
mkdir data
mkdir data/alpha_vantage
mkdir data/reddit
mkdir jobs
mkdir jobs/logs

sudo apt install python3-pip
pip3 install -r requirements.txt
# $ sudo crontab -e
# * * * * * /usr/bin/sh /home/petiteai519/run_job.sh
