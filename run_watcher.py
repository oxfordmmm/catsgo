doc = """watches for new finished runs and submits pipeline scientific data to oracle apex vis
"""

import csv
import datetime
import json
import logging
import os
import time
from collections import defaultdict
from glob import glob
from pathlib import Path

import argh
import gridfs
import pymongo
import requests

import catsgo
import utils
import db
import sentry_sdk

config = utils.load_config("config.json")

def before_send(event, hint):
    if 'exc_info' in hint:
        exc_type, exc_value, tb = hint['exc_info']
        if exc_value.args[0] in ["The "+make_viridian_sample_header.vn+" file could not be found"]:
            return None
    return event

sentry_sdk.init(
    dsn=config["sentry_dsn_run_watcher"],

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=config["sentry_traces_sample_rate"],
        before_send=before_send
)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["dir_watcher"]
metadata = mydb["metadata"]
runlist = mydb["runlist"]


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_submitted_runlist(pipeline_name):
    ret = runlist.find_one({"pipeline_name": pipeline_name}, {"finished_uuids": 1})
    if not ret:
        return list()
    else:
        return list(ret.get("finished_uuids", list()))


def add_to_submitted_runlist(pipeline_name, new_run_uuid):
    runlist.update_one(
        {"pipeline_name": pipeline_name},
        {"$push": {"finished_uuids": new_run_uuid}},
        upsert=True,
    )


def save_sample_data(new_run_uuid, sp3_sample_name, sample_data):
    metadata.update_one(
        {"run_uuid": new_run_uuid},
        {"$push": {"submitted_sample_data": {sp3_sample_name: sample_data}}},
        upsert=True,
    )


def get_data_for_run(new_run_uuid):
    data = metadata.find_one({"run_uuid": new_run_uuid}, {"_id": 0})
    return data


def get_sample_map_for_run(new_run_uuid):
    data = get_data_for_run(new_run_uuid)

    if not data:
        return None

    ret = dict()
    try:
        for s in data["apex_samples"]["samples"]:
            ret[s["name"]] = s["id"]
    except Exception as e:
        logging.error(f"get_sample_map_for_run: {str(e)}")
        return None

    return json.dumps(ret)


def make_viridian_sample_header(new_run_uuid, sp3_sample_name):
    """
        Args:
            new_run_uuid (uuid): The unique id for the current run
            sp3_sample_name: The name of the SP3 sample
        Returns:
            dict: An object containing a subset of information from the Viridian log 
            file for the run. If no log file could be found None is return
    """
    log = None
    vn = (
        Path("/work/output") 
        / new_run_uuid 
        / "qc" 
        / f"{sp3_sample_name}.json"
    )
    if not vn.is_file():
        logging.error(f"The {vn} file could not be found")
        return None
    else:
        log = {}
        with open(vn) as f:
            log["viridian_log"] = json.load(f)
    
    if log:
        return log
    else:
        logging.error(f"The {vn} file is empty")
        return None


def make_sample_data(new_run_uuid, sp3_sample_name):
    log = make_viridian_sample_header(new_run_uuid, sp3_sample_name)
    if not log:
        return None
    sample = {}
    fn = None
    fn1json = (
        Path("/work/output")
        / new_run_uuid
        / "analysis/report/illumina"
        / f"{sp3_sample_name}_report.json"
    )
    fn2json = (
        Path("/work/output")
        / new_run_uuid
        / "analysis/report/nanopore"
        / f"{sp3_sample_name}_report.json"
    )
    if fn1json.is_file() and fn2json.is_file():
        logging.error(
            f"Both {fn1json} and {fn2json} present. Expected only one. Exiting"
        )
        return log
    if fn1json.is_file():
        fn = fn1json
    elif fn2json.is_file():
        fn = fn2json

    if fn:
        with open(fn) as report:
            sample = json.load(report)
            return {**log, **sample}
    else:
        logging.warning(
            f"Could not find JSON reports {fn1json} or {fn2json}, trying TSV outputs."
        )

        fn1tsv = (
            Path("/work/output")
            / new_run_uuid
            / "analysis/report/illumina"
            / f"{sp3_sample_name}_report.tsv"
        )
        fn2tsv = (
            Path("/work/output")
            / new_run_uuid
            / "analysis/report/nanopore"
            / f"{sp3_sample_name}_report.tsv"
        )
        if not (fn1tsv.is_file() or fn2tsv.is_file()):
            logging.warning(
                f'Neither files "{fn1tsv}", "{fn2tsv}" could be found. Check pipeline for sample failure.'
            )
            return log
        if fn1tsv.is_file() and fn2tsv.is_file():
            logging.error(
                "Both {fn1tsv} and {fn2tsv} present. Expected only one. Exiting"
            )
            return log
        if fn1tsv.is_file():
            fn = fn1tsv
        else:
            fn = fn2tsv

    if fn != None:
        logging.info(f"opening analysis report: {fn}")
        with open(fn) as report:

            results = csv.DictReader(report, delimiter="\t")
            sample = {
                "pipelineDescription": "Pipeline Description",  # "Pipeline Description"
                "vocVersion": "",  # row["phe-label"]
                "vocPheLabel": "VOC-20DEC-01",  # row["unique-id"]
                "assemblies": [],
                "vcfRecords": [],
                "variants": [],
            }

            for row in results:
                sample["lineageDescription"] = row["lineage"]
                sample["pipelineVersion"] = row["version"]
                if "aln2type_variant_version" in row:
                    sample["vocVersion"] = row["aln2type_variant_version"]

                for i in row["aaSubstitutions"].split(","):
                    try:
                        gene, name = i.split(":")
                        sample["variants"].append({"gene": gene, "name": name})
                    except ValueError as e:
                        logging.error(f"parse error: str(e)")

        return {**log, **sample}
    else:
        logging.error("Could not find report files. Exiting")
        return log


def submit_sample_data_error(
    error_str="Error: unspecified error",
    apex_database_sample_name=None,
    sp3_sample_name=None,
    sp3_run_uuid=None,
    apex_token=None,
    config=None,
):
    """Given a oracle sample id or sp3 sample name, submit sample error status."""

    if not apex_database_sample_name and not (sp3_sample_name and sp3_run_uuid):
        logging.error(
            "submit_sample_data_error: You need to provide either the oracle sample id or the sp3 sample name"
        )
        return
    if sp3_sample_name and sp3_run_uuid:
        m = get_sample_map_for_run(sp3_run_uuid)
        if not m:
            logging.error("submit_sample_data_error: couldn't get map for sample")
            return
        apex_database_sample_name = m.get(sp3_sample_name)
        if not apex_database_sample_name:
            logging.error(
                "submit_sample_data_error: couldn't find {sp3_sample_name} in map"
            )
            return

    if not apex_token:
        apex_token = db.get_apex_token()
    if not config:
        config = utils.load_oracle_config("config.json")

    if type(error_str) != str:
        # just in case
        error_str = str(error_str)

    data = {
        "sample": {
            "operations": [
                {
                    "op": "replace",
                    "path": "errorMsg",
                    "value": error_str,
                },
                {"op": "replace", "path": "status", "value": "Error"},
            ]
        }
    }

    sample_data_response = requests.put(
        f"{config['host']}/samples/{apex_database_sample_name}",
        headers={"Authorization": f"Bearer {apex_token}"},
        json=data,
    )
    logging.info(f"POSTing error to {config['host']}/samples/{apex_database_sample_name}")
    return sample_data_response.text


def submit_sample_data(apex_database_sample_name, data, config, apex_token):
    data = {
        "sample": {"operations": [{"op": "add", "path": "analysis", "value": [data]}]}
    }
    sample_data_response = requests.put(
        f"{config['host']}/samples/{apex_database_sample_name}",
        headers={"Authorization": f"Bearer {apex_token}"},
        json=data,
    )
    logging.info(f"POSTing to {config['host']}/samples/{apex_database_sample_name}")
    return sample_data_response.text


def send_output_data_to_api(new_run_uuid, config, apex_token):
    sample_map = get_sample_map_for_run(new_run_uuid)
    if not sample_map:
        return
    sample_map = json.loads(sample_map)

    for sp3_sample_name, apex_database_sample_name in sample_map.items():
        logging.info(
            f"processing sample {sp3_sample_name} (oracle id: {apex_database_sample_name})"
        )
        sample_data = make_sample_data(new_run_uuid, sp3_sample_name)

        if sample_data:
            r = submit_sample_data(
                apex_database_sample_name, sample_data, config, apex_token
            )
            logging.info(f"received {r}")
        else:
            logging.warning(
                f"Couldn't get sample data for {sp3_sample_name} (oracle id: {apex_database_sample_name})"
            )
            submit_sample_data_error(
                "Couldn't get sample data",
                apex_database_sample_name=apex_database_sample_name,
            )


def process_run(new_run_uuid, config, apex_token):
    send_output_data_to_api(new_run_uuid, config, apex_token)


def get_finished_ok_sp3_runs(pipeline_name):
    return (
        catsgo.get_all_runs2(pipeline_name)
        .get("status_to_run_uuid", dict())
        .get("OK", list())
    )


def watch(flow_name="oxforduni-gpas-sars-cov2-illumina"):
    apex_token = None
    apex_token_time = 0
    config = utils.load_oracle_config("config.json")

    while True:
        # get a new token every 5 hours (& startup)
        time_now = int(time.time())
        apex_token_age = time_now - apex_token_time
        if apex_token_age > 5 * 60 * 60:
            logging.info(f"Acquiring new token (token age: {apex_token_age}s)")
            apex_token = db.get_apex_token()
            apex_token_time = time_now

        # new runs to submit are sp3 runs that have finished with status OK
        # minus runs that have been marked as already submitted
        finished_ok_sp3_runs = set(get_finished_ok_sp3_runs(flow_name))
        submitted_runs = set(get_submitted_runlist(flow_name))
        new_runs_to_submit = finished_ok_sp3_runs.difference(submitted_runs)

        for new_run_uuid in new_runs_to_submit:
            logging.info(f"new run: {new_run_uuid}")
            process_run(new_run_uuid, config, apex_token)
            add_to_submitted_runlist(flow_name, new_run_uuid)

        logging.info("sleeping for 60")
        time.sleep(60)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands(
        [
            watch,
            make_sample_data,
            get_sample_map_for_run,
            submit_sample_data,
            submit_sample_data_error,
        ]
    )
