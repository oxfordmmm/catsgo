doc = """
watches a directory and runs a pipeline on any new subdirectories
submits metadata to api
"""

import logging
import os
import time
from pathlib import Path
import csv
import datetime
import json
from collections import defaultdict

import argh
import gridfs
import pymongo
import requests

import catsgo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["dir_watcher"]
dirlist = mydb["dirlist"]
metadata = mydb["metadata"]


def get_cached_dirlist(watch_dir):
    ret = dirlist.find_one({"watch_dir": watch_dir}, {"dirs": 1})
    if not ret:
        return list()
    else:
        return list(ret.get("dirs"))


def add_to_cached_dirlist(watch_dir, new_dir, run_uuid):
    logging.debug(f"adding {new_dir}")
    dirlist.update_one(
        {"watch_dir": watch_dir}, {"$push": {"dirs": new_dir}}, upsert=True
    )
    metadata.update_one(
        {"catsup_uuid": new_dir},
        {
            "$set": {
                "run_uuid": run_uuid,
                "added_time": str(int(time.time())),
            }
        },
        upsert=True,
    )


def send_metadata_to_api(watch_dir, new_dir, apex_metadata_endpoint):
    data_file = Path(watch_dir) / new_dir / "sp3data.csv"
    if not data_file.is_file():
        logging.error("send_metadata_to_api(): {data_file} not a file")
        return

    with open(data_file, newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        logging.error("send_metadata_to_api(): {data_file} no rows")
        return

    out = {"batch": {"samples": list()}}

    rows_by_sample = defaultdict(list)
    for row in rows:
        rows_by_sample[row.get("sample_uuid4")].append(row)

    seen_sample_uuid4s = set()
    for row in rows:
        if row.get("sample_uuid4") in seen_sample_uuid4s:
            continue
        seen_sample_uuid4s.add(row.get("sample_uuid4"))

        metadata = {
            "fileName": "",
            "uploadedOn": datetime.datetime.now().isoformat() + "Z",
            "uploadedBy": row.get("submitter_email", ""),
            "organisation": row.get("submitter_organisation", ""),
            "site": row.get("submitter_site", ""),
            "errorMsg": "",
        }
        p = {
            "errorMsg": "",
            "name": row.get("sample_uuid4", ""),
            "host": row.get("sample_host", ""),
            "collectionDate": row.get("sample_collection_date", ""),
            "country": row.get("sample_country", ""),
            "fileName": "",
            "specimenOrganism": row.get("sample_organism", ""),
            "specimenSource": row.get("sample_source", ""),
            "status": "Uploaded",
            "submissionTitle": row.get("submission_title", ""),
            "submissionDescription": row.get("submission_description", ""),
            "instrument": {
                "platform": row.get("instrument_platform", ""),
                "model": row.get("instrument_model", ""),
                "flowcell": row.get("instrument_flowcell", ""),
            },
        }
        rows_for_sample = rows_by_sample.get(row.get("sample_uuid4"))
        if len(rows_for_sample) == 1:
            p["seReads"] = [{ "uri": "",
                              "sp3_filepath": str(Path(watch_dir) / new_dir / rows_for_sample[0].get("sample_filename")),
                              "md5": rows_for_sample[0].get("clean_file_md5", "") }]
        if len(rows_for_sample) == 2:
            p["peReads"] = [{ "r1_uri": "",
                              "r1_sp3_filepath": str(Path(watch_dir) / new_dir / rows_for_sample[0].get("sample_filename")),
                              "r1_md5": rows_for_sample[0].get("clean_file_md5", ""),
                              "r2_uri": "",
                              "r2_sp3_filepath": str(Path(watch_dir) / new_dir / rows_for_sample[1].get("sample_filename")),
                              "r2_md5": rows_for_sample[1].get("clean_file_md5", "") }]


        out["batch"]["samples"].append(p)
    for k, v in metadata.items():
        out["batch"][k] = v

    # post out to api

    print(json.dumps(out, indent=4))


def remove_from_cached_dirlist(watch_dir, new_dir):
    logging.debug(f"removing {new_dir}")
    dirlist.update_one(
        {"watch_dir": watch_dir}, {"$pull": {"dirs": new_dir}}, upsert=True
    )


def process_dir(new_dir, watch_dir, pipeline, flow_name, bucket_name, apex_metadata_endpoint):
    if not (watch_dir / new_dir).is_dir():
        logging.warning(f"dir_watcher: {new_dir}: expected a directory, found a file")
        return
    if not (watch_dir / new_dir / "upload_done.txt").is_file():
        logging.info(f"dir_watcher: {new_dir} upload in progress?")
        return
    if pipeline == "covid_illumina":
        try:
            ret = catsgo.run_covid_illumina_catsup(
                flow_name, str(watch_dir / new_dir), bucket_name, new_dir
            )
            logging.info(ret)
            # add to it list of stuff already run
            add_to_cached_dirlist(str(watch_dir), new_dir, ret.get("run_uuid", ""))
        except Exception as e:
            logging.error(
                f"dir_watcher: pipeline run exception: pipeline: {pipeline}, new_dir: {new_dir}, watch_dir: {str(watch_dir)}, exception: {str(e)}"
            )
        if apex_metadata_endpoint:
            try:
                send_metadata_to_api(flow_name, watch_dir, apex_metadata_endpoint)
            except Exception as e:
                logging.error(
                    f"dir_watcher: pipeline run exception: pipeline: {pipeline}, new_dir: {new_dir}, watch_dir: {str(watch_dir)}, exception: {str(e)}"
                )


def watch(watch_dir, pipeline, flow_name, bucket_name, apex_metadata_endpoint=""):
    print(doc)
    watch_dir = Path(watch_dir)
    if not watch_dir.is_dir():
        logging.error(f"{watch_dir} is not a directory")
        os.exit(1)

    while True:
        candidate_dirs = set([x.name for x in Path(watch_dir).glob("*") if x.is_dir()])
        cached_dirlist = set(get_cached_dirlist(str(watch_dir)))
        new_dirs = candidate_dirs.difference(cached_dirlist)

        for new_dir in new_dirs:
            process_dir(new_dir, watch_dir, pipeline, flow_name, bucket_name)

        logging.debug("sleeping for 60")
        time.sleep(60)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands([watch, remove_from_cached_dirlist, send_metadata_to_api])
