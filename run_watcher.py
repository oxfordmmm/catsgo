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
from db import Config

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
        logging.error("get_sample_map_for_run: {str(e)}")
        return None

    return json.dumps(ret)


def get_apex_token():
    with open("secrets.json") as f:
        c = json.load(f)
        client_id = c.get("client_id")
        client_secret = c.get("client_secret")

    config = Config("config.ini")

    access_token_response = requests.post(
        config.idcs,
        data={
            "grant_type": "client_credentials",
            "scope": config.host,
        },
        allow_redirects=False,
        auth=(client_id, client_secret),
    )
    access_token = access_token_response.json().get("access_token")
    if not access_token:
        logging.error(f"Error generating token: {access_token_response.text}")
        exit(1)
    return access_token


def make_sample_data(new_run_uuid, sp3_sample_name):
    sample = {}
    fn1 = (
        Path("/work/output")
        / new_run_uuid
        / "analysis/report/illumina"
        / f"{sp3_sample_name}_report.tsv"
    )
    fn2 = (
        Path("/work/output")
        / new_run_uuid
        / "analysis/report/nanopore"
        / f"{sp3_sample_name}_report.tsv"
    )
    if not (fn1.is_file() or fn2.is_file()):
        logging.warning(
            f'Neither files "{fn1}", "{fn2}" could be found. Check pipeline for sample failure.'
        )
        return None
    if fn1.is_file() and fn2.is_file():
        logging.error("Both {fn1} and {fn2} present. Expected only one.")
        # but continue anyway
    if fn1.is_file():
        fn = fn1
    if fn2.is_file():
        # if both exist we're going with ~~~OXFORD~~~
        fn = fn2

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

            for i in row["aaSubstitutions"].split(","):
                try:
                    gene, name = i.split(":")
                    sample["variants"].append({"gene": gene, "name": name})
                except ValueError as e:
                    logging.error(f"parse error: str(e)")

    return sample


def submit_sample_data(apex_database_sample_name, data, config, apex_token):
    data = {
        "sample": {"operations": [{"op": "add", "path": "analysis", "value": [data]}]}
    }
    sample_data_response = requests.put(
        f"{config.host}/samples/{apex_database_sample_name}",
        headers={"Authorization": f"Bearer {apex_token}"},
        json=data,
    )
    logging.info(f"POSTing to {config.host}/samples/{apex_database_sample_name}")
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


def process_run(new_run_uuid, config, apex_token):
    send_output_data_to_api(new_run_uuid, config, apex_token)


def get_finished_ok_sp3_runs(pipeline_name):
    return (
        catsgo.get_all_runs2(pipeline_name)
        .get("status_to_run_uuid", dict())
        .get("OK", list())
    )


def watch(flow_name="oxforduni-ncov2019-artic-nf-illumina"):
    apex_token = None
    apex_token_time = 0
    config = Config("config.ini")

    while True:
        # get a new token every 5 hours (& startup)
        time_now = int(time.time())
        apex_token_age = time_now - apex_token_time
        if apex_token_age > 5 * 60 * 60:
            logging.info(f"Acquiring new token (token age: {apex_token_age}s)")
            apex_token = get_apex_token()
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
    argh.dispatch_commands([watch, make_sample_data, submit_sample_data])
