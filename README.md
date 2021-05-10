# catsgo

CLI client for SP3

## Requirements: 

Python 3.6+

## Installation

$ virualenv -p python3 env
$ source env/bin/activate
$ pip3 install -r requirements.txt

## Setup

### Setup TB pipeline
Copy `config.json-example` to `config.json` and edit it

### Setup Covid pipeline
Copy `config.json-covid` to `config.json` and edit it

You must edit at least `username`, `password` and `sp3_url`

The rest of the configuration relates to running the TB or Covid pipeline in an automated way.

Since this file contains your login details, please ensure that it is only readable by you.

## Run TB pipeline
### Fetch, run and download output

    $ python3 catsgo.py go <fetch_path>

## Download TB reports

### Download report of one sample

    $ python3 catsgo.py download-report {run_uuid} {sample_name} > one_report.json


### Download reports of a run

    $ python3 catsgo.py download-reports {pipeline_name} {run_uuid} > run_reports.json

### Transform json report to csv format (with output of above)

    $ python3 report2csv.py run_reports.json > run_reports.csv

## Run Covid pipeline

- Illumina pipeline

    $ python3 catsgo.py run-covid-illumina <pipeline_name> <data_path>

## Issues

It is possible (but unlikely) that the sp3 server will clear your session between the login and the next request resulting in an unhandled failure.

Note that the report functions run after a run is finished. Therefore it is not guarateed that all reports will be finished just because a run is done. Some reports may take a long time to run. You should check if your required report type is present, and if not, re-queue your request.
