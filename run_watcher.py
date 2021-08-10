doc = """watches for new finished runs and submits pipeline scientific data to oracle apex vis
"""

import csv
import datetime
import json
import logging
import os
import time
from collections import defaultdict
from pathlib import Path

import argh
import gridfs
import pymongo
import requests

from db import get_analysis, Config
from glob import glob

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["dir_watcher"]
metadata = mydb["metadata"]
runlist = mydb["runlist"]


def get_finished_ok_sp3_runs(pipeline_name):
    pass


#    return (
#        catsgo.get_all_runs2(pipeline_name)
#        .get("status_to_run_uuid", dict())
#        .get("OK", list())
#    )


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
    # logging.info(f"getting data for run {data}")

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
        data={"grant_type": "client_credentials", "scope": config.host,},
        #    verify=False,
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
    fn = (
        Path("/work/output")
        / new_run_uuid
        / "analysis/report/illumina"
        / f"{sp3_sample_name}_report.tsv"
    )
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
            logging.info(f"lineage call: {row['lineage']}")
            sample["lineageDescription"] = row["lineage"]
            sample["pipelineVersion"] = row["version"]
            sample["status"] = "Ready to Release"

            for i in row["aaSubstitutions"].split(","):
                gene, name = i.split(":")
                sample["variants"].append({"gene": gene, "name": name})

            # let's get this working with the first row of analysis results for now
            # should just have to skip E483K or whatever, really

    # all outputs are in /work/output/<run_uuid>
    # for example:
    #
    # /work/output/c0ac178c-0b4c-438f-8080-046f4d638d9e/analysis/pango/illumina/150b5feb-61e1-4b6d-892a-170e6be09f71_lineage_report.csv
    # /work/output/c0ac178c-0b4c-438f-8080-046f4d638d9e/analysis/report/illumina/analysisReport.tsv
    # /work/output/c0ac178c-0b4c-438f-8080-046f4d638d9e/analysis/report/illumina/150b5feb-61e1-4b6d-892a-170e6be09f71_report.json
    # /work/output/c0ac178c-0b4c-438f-8080-046f4d638d9e/analysis/report/illumina/150b5feb-61e1-4b6d-892a-170e6be09f71_report.tsv
    # /work/output/c0ac178c-0b4c-438f-8080-046f4d638d9e/analysis/aln2type/illumina/150b5feb-61e1-4b6d-892a-170e6be09f71.csv
    # /work/output/c0ac178c-0b4c-438f-8080-046f4d638d9e/analysis/nextclade/illumina/150b5feb-61e1-4b6d-892a-170e6be09f71.tsv
    # /work/output/c0ac178c-0b4c-438f-8080-046f4d638d9e/consensus_seqs/150b5feb-61e1-4b6d-892a-170e6be09f71.fasta
    #
    return sample


def submit_sample_data(apex_database_sample_name, data, config):
    data = {
        "sample": {"operations": [{"op": "add", "path": "analysis", "value": [data]}]}
    }
    sample_data_response = requests.put(
        f"{config.host}/samples/{apex_database_sample_name}",
        headers={"Authorization": f"Bearer {config.token}"},
        json=data,
    )
    logging.info(f"POSTing to {config.host}/samples/{apex_database_sample_name}")
    logging.info(f"{data}")
    return sample_data_response.text


def send_output_data_to_api(new_run_uuid, config):
    sample_map = get_sample_map_for_run(new_run_uuid)
    # logging.info(f"sample map {sample_map}")
    if not sample_map:
        return
    sample_map = json.loads(sample_map)

    for sp3_sample_name, apex_database_sample_name in sample_map.items():
        logging.info(
            f"processing sample {sp3_sample_name} (oracle id: {apex_database_sample_name})"
        )
        sample_data = make_sample_data(new_run_uuid, sp3_sample_name)
        logging.info(sample_data)
        # Consider:
        # save_sample_data(new_run_uuid, sp3_sample_name, sample_data)

        r = submit_sample_data(apex_database_sample_name, sample_data, config)
        logging.info(f"received {r}")


def process_run(new_run_uuid, config):
    send_output_data_to_api(new_run_uuid, config)


def analysis_complete(run_uuid):
    return Path(
        f"/work/output/{run_uuid}/analysis/report/illumina/analysisReport.tsv"
    ).exists()


def get_run_uuids(pattern="/work/output/*/analysis/report/illumina/analysisReport.tsv"):
    for report in glob(pattern):
        yield report.split("/")[3]


def config_gpas(apex_token):
    conf = Config("config.ini")
    conf.token = apex_token
    return conf


def watch(flow_name="oxforduni-ncov2019-artic-nf-illumina"):
    apex_token = get_apex_token()  # run this every ten hours
    logging.info(f"using apex token {apex_token}")
    config = config_gpas(apex_token)
    while True:
        # work-around because this requires an sp3 api call:
        # finished_ok_sp3_runs = set(get_finished_ok_sp3_runs(flow_name))
        new_runs_to_submit = set()
        run_uuids = list(get_run_uuids())
        nsamples = 0
        for run_uuid in run_uuids:
            s = get_sample_map_for_run(run_uuid)
            if not s:
                continue
            s = json.loads(s)
            for guid, oracle_id in s.items():
                nsamples += 1
                # logging.info(f"{run_uuid} {guid} {oracle_id}")
                analyses = get_analysis(oracle_id, config=config)
                if analyses is None:
                    logging.info(
                        f"failed to fetch analyses for {oracle_id}. bad token?"
                    )
                    continue
                analyses = analyses[0]
                if len(analyses) > 0:
                    # logging.info(
                    # f"run already had analyses: {run_uuid} ({oracle_id}) analyses: {analyses}"
                    # )
                    all_null = True
                    for analysis in analyses:
                        # logging.info(f"analysis: {analysis}")
                        if (
                            analysis["pipelineVersion"] is not None
                            and analysis["pipelineVersion"] != "Pipeline Version"
                        ):
                            all_null = False
                    if all_null:
                        # logging.info(f"all analyses are null: {analyses}")
                        new_runs_to_submit.add(run_uuid)

                if len(analyses) == 0:
                    new_runs_to_submit.add(run_uuid)

        logging.info(
            f"there are {len(run_uuids)} runs. {nsamples} samples. {len(new_runs_to_submit)} new runs to submit."
        )
        #        submitted_runs = set(get_submitted_runlist(flow_name))

        if new_runs_to_submit:
            pass
            # generate a new token here if < N hours have elapsed

        for new_run_uuid in new_runs_to_submit:
            logging.info(
                f"new run: {new_run_uuid}, complete: {analysis_complete(new_run_uuid)}"
            )
            process_run(new_run_uuid, config)
            add_to_submitted_runlist(flow_name, new_run_uuid)

        logging.info("sleeping for 60")
        time.sleep(60)
        config.token = get_apex_token()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands([watch, make_sample_data, submit_sample_data])
