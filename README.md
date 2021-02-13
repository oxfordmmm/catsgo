# catsgo

CLI client for SP3

# Requirements

- python 3.6 with pip3

# Installation

$ pip3 install -r requirements.txt

# Setup

Copy `config.json-example` to `config.json` and edit it

You must edit at least `username`, `password` and `sp3_url`

The rest of the configuration relates to running the clockwork pipeline in an automated way

Since this file contains your login details, please ensure that it is only readable by you.

# Running

- Download report:

    $ python3 catsgo.py download-report <run_uuid> <sample_name>

- Download reports for run:

    $ python3 catsgo.py download-reports <pipeline_name> <run_uuid>

# Issues

It is possible (but unlikely) that the sp3 server will clear your session between the login and the next request resulting in an unhandled failure.

Note that the report functions run after a run is finished. Therefore it is not guarateed that all reports will be finished just because a run is done. Some reports may take a long time to run. You should check if your required report type is present, and if not, re-queue your request.