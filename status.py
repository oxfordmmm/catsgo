#!/usr/bin/python
import time
import os
import subprocess
import sentry_sdk

config = utils.load_config("config.json")

sentry_sdk.init(
    dsn=config["sentry_dsn_status"],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0
)

# Check that the buckets are mounted
while 1 != 0:
    with open('/home/ubuntu/catsgo/buckets.txt') as buckets:
        for line in buckets:
            line = line.replace(',', '')
            line = line.replace('\n', '')
            mstatus = os.path.ismount("/data/inputs/s3/"+line.rstrip())
            if mstatus == False:
                try:
                    raise Exception('Mount ' + line + ' is not available, please check!')
                except Exception as error:
                    print('Caught this error: ' + repr(error))
                    sentry_sdk.capture_message('Mount ' + line + ' is not available, please check!')

# check that the dir_watchers are running
with open('/home/ubuntu/catsgo/buckets.txt') as processes:
        for line in processes:
            line = line.replace(',', '')
            line = line.replace('\n', '')
            pstatus = os.system('systemctl is-active --user dir_watcher@'+line.rstrip()+'.service')
            if pstatus != 0:
                try:
                    raise Exception('dir_watcher ' + line + ' is not active, please check!')
                except Exception as error:
                    print('Caught this error: ' + repr(error))
                    sentry_sdk.capture_message('systemctl is-active --user dir_watcher@'+line.rstrip()+'.service')

# check that illumina run_watcher is running
rstatus1 = os.system('systemctl is-active --user run_watcher@oxforduni-gpas-sars-cov2-illumina.service')
if rstatus1 != 0:
    try:
        raise Exception('run_watcher@oxforduni-gpas-sars-cov2-illumina.service is not active, please check!')
    except Exception as error:
        print('caught this error: ' + repr(error))
        sentry_sdk.capture_message('run_watcher@oxforduni-gpas-sars-cov2-illumina.service is not active, please check!')

# check that nanopore run_watcher is running
rstatus2 = os.system('systemctl is-active --user run_watcher@oxforduni-gpas-sars-cov2-nanopore.service')
if rstatus2 != 0:
    try:
        raise Exception('run_watcher@oxforduni-gpas-sars-cov2-nanopore.service is not active, please check!')
    except Exception as error:
        print('Caught this error: ' + repr(error))
        sentry_sdk.capture_message('run_watcher@oxforduni-gpas-sars-cov2-nanopore.service is not active, please check!')

if config["using_ENA_runner"]:
    rstatus3 = os.system(f'systemctl is-active --user ena_runner@{config["ENA_bucket"]}.service')
    if rstatus3 != 0:
        try:
            raise Exception(f'ena_runner@{config["ENA_bucket"]}.service is not active, please check!')
        except Exception as error:
            print('Caught this error: ' + repr(error))
            sentry_sdk.capture_message(
                f'ena_runner@{config["ENA_bucket"]}.service is not active, please check!')

time.sleep(360)
