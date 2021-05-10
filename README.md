# catsgo

CLI client for SP3

## Requirements: 

Python 3.6+

## Installation

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

    e.g $ python3 catsgo.py go /data/inputs/system/sp3_test_data 

## Download TB reports

### Download report of one sample

    $ python3 catsgo.py download-report {run_uuid} {sample_name} > one_report.json

    e.g. $ python3 catsgo.py download-report 87b3e6dc-9b6c-42f2-8574-0a1eab0f6c90 SRR7800670 > SRR7800670.json

### Download reports of a run

    $ python3 catsgo.py download-reports {pipeline_name} {run_uuid} > run_reports.json
    
    e.g. $ python3 catsgo.py download-reports  oxforduni-Clockwork_combined 87b3e6dc-9b6c-42f2-8574-0a1eab0f6c90 > run_87b3e6dc.json

### Transform json report to csv format (with above output)

    $ python3 report2csv.py run_reports.json > run_reports.csv
    
    e.g. $ python3 report2csv.py run_87b3e6dc.json > run_87b3e6dc.csv

## Run Covid pipeline

- Illumina pipeline

    $ python3 catsgo.py run-covid-illumina {pipeline_name} {data_path}

## Issues

It is possible (but unlikely) that the sp3 server will clear your session between the login and the next request resulting in an unhandled failure.

Note that the report functions run after a run is finished. Therefore it is not guarateed that all reports will be finished just because a run is done. Some reports may take a long time to run. You should check if your required report type is present, and if not, re-queue your request.
