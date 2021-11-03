#!/env/bin/python3

doc = """
watches a directory which contains COVID ENA uploaded data and 
batches them to be run by catsgo
"""

import csv
import logging
import time
import uuid
import json
import datetime
from pathlib import Path

import argh
import pymongo
import requests


import db
import catsgo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["ena_runner"]
dirlist = mydb["dirlist"]

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_ena_metadata(sample):
    url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?accession={sample}&fields=host%2Cscientific_name%2Cfirst_public%2Ccollection_date%2Cinstrument_platform%2Cinstrument_model%2Ccountry&format=json&limit=10&result=read_run"
    response = requests.get(url)
    try:
        j = response.json()
        if len(j) == 1:
            return j[0]
        else:
            logging.error(f"requested more than a single sample: accession {sample}, json {j}")
    except:
        logging.error(f"empty response from host: {url}")
        return None

def get_cached_dirlist(sample_method, path):
    """
    get the list of Samples that have already run from a sample_method (illumina or nanopore) and path (prefix/shard)
    """
    return list(dirlist.find({"sample_method": sample_method, "path": path}, {"samples": 1}))

def add_to_cached_dirlist(sample_method, path, samples):
    """
    add the samples to the list of samples that have batched
    """

    logging.debug(f"adding {samples} to {sample_method}, {path}")
    dirlist.update_one(
        {"sample_method": sample_method, "path": path}, {"$push": {"samples": { "$each" : samples }}}, upsert=True
    )

def process_batch(sample_method, samples_to_submit, batch_dir):
    print(f"processing {samples_to_submit}")
    samples = list()
    sample_shards = dict()
    batch_name = "ENA-" + str(uuid.uuid4())[:7]
    submission_name = f"Entry for ENA sample processing - {batch_name}"

    for sample in samples_to_submit:
        ena_metadata = get_ena_metadata(sample.name)
        p = {
            "name": sample.name,
            "submissionTitle": submission_name,
            "submissionDescription": submission_name,
            #"specimenOrganism" : ena_metadata["scientific_name"],
            "status": "Uploaded",
            "instrument": {
                "platform": ena_metadata['instrument_platform'],
                "model": ena_metadata['instrument_model'],
                "flowcell": 0,
            },
        }

        if ena_metadata["collection_date"] != "":
            p["collectionDate"] = ena_metadata["collection_date"]
        elif ena_metadata["first_public"]:
            p["collectionDate"] = ena_metadata["first_public"]

        if ena_metadata["country"] != "":
            p["country"] = ena_metadata["country"]
        else:
            p["country"] = "United Kingdom"

        if ena_metadata["scientific_name"] == "Severe acute respiratory syndrome coronavirus 2": 
            p["specimenOrganism"] = "SARS-CoV-2"
        else:
            p["specimenOrganism"] = ena_metadata["scientific_name"]

        if sample_method.name == "illumina":
            p["peReads"] = [
                {
                    "r1_sp3_filepath": str(Path(sample) / (sample.name+"_1.fastq.gz")),
                    "r2_sp3_filepath": str(Path(sample) / (sample.name+"_2.fastq.gz")),
                }
            ]
            p["seReads"] = []
        elif sample_method.name == "nanopore":
            p["seReads"] = [
                {
                    "sp3_filepath": str(Path(sample) / (sample.name+"_1.fastq.gz")),
                }
            ]
            p["peReads"] = []
        else:
            logging.error(f"Invalid sample_method {sample_method}")
        samples.append(p)
        # Add to dict for submitting to cached_dirs
        path = sample.relative_to(sample_method)
        path = str(path.parent)
        if path in sample_shards:
            sample_shards[path].append(sample.name)
        else:
            sample_shards[path] = [sample.name]
    
    submission = {"batch": {
            "fileName" : batch_name,
            "bucketName" : sample_method.parent.name,
            "organisation" : "Public Repository Data",
            "site": "ENA Data",
            "uploadedOn": datetime.datetime.now().isoformat()[:-3] + "Z",
            "uploadedBy": "Jeremy.Swann@ndm.ox.ac.uk",
            "samples": samples,
        }
    }

    apex_token = db.get_apex_token()
    apex_batch, apex_samples = db.post_metadata_to_apex(submission, apex_token)

    for path, sample_list in sample_shards.items():
        add_to_cached_dirlist(sample_method.name, path, sample_list)
    
    # Add to batch_dir
    ena_batch_csv = Path(batch_dir) / f"{batch_name}.csv"
    out_fieldnames = ['bucket','sample_prefix','sample_accession']
    with open(ena_batch_csv, 'w') as out_csv:
        writer1 = csv.DictWriter(out_csv, fieldnames=out_fieldnames)
        writer1.writeheader()
        for sample in samples_to_submit:
            out = {
                'bucket' : submission['batch']['bucketName'],
                'sample_prefix' : Path(sample),
                'sample_accession' : sample.name
            }
            writer1.writerow(out)

    ret = catsgo.run_covid_ena(
        f"oxforduni-ncov2019-artic-nf-{sample_method.name}",
        str(ena_batch_csv),
        batch_name)

    return []

def watch(
    watch_dir="",
    batch_dir="",
    size_batch=10
):
    """
    
    """
    print(doc)
    watch_dir = Path(watch_dir)
    if not watch_dir.is_dir():
        logging.error(f"{watch_dir} is not a directory")
        sys.exit(1)

    while True:
        # ENA bucket will have illumina and nanopore data
        for sample_method in [ watch_dir / "illumina", watch_dir / "nanopore"]:
            samples_to_submit = []
            # These are the prefixs of the sample asscessions (E.G. ERR408 has, in shards (000-010), samples ERR4080000 - ERR4089999)
            for prefix_dir in set([x.name for x in sample_method.glob("*") if x.is_dir()]):
                # Go through each shard seperately
                for shard_dir in set([y.name for y in ( watch_dir / sample_method / prefix_dir ).glob("*") if y.is_dir()]):
                    # Get all sample directories (each of which should only have one sample!)
                    candidate_dirs = set([z.name for z in ( watch_dir / sample_method / prefix_dir / shard_dir ).glob("*") if z.is_dir() and len(set(( watch_dir / sample_method / prefix_dir / shard_dir / z ).glob("*"))) == 2 ])
                    # get directories/submissions that have already been processed
                    cached_dirlist = set(get_cached_dirlist(sample_method.name, str( Path(prefix_dir) / shard_dir)))
                    # submissions to be processed are those that are new and have not beek marked as failed
                    new_dirs = candidate_dirs.difference(cached_dirlist)
                    #print(f"{sample_method} - {prefix_dir} - {shard_dir} - {new_dirs}")

                    if new_dirs:
                        new_dirs = list(new_dirs)
                        # Check if submitting
                        while len(new_dirs) + len(samples_to_submit) >= size_batch:
                            samples_to_submit += [watch_dir / sample_method / prefix_dir / shard_dir / z for z in new_dirs[:size_batch - len(samples_to_submit)]]
                            samples_to_submit = process_batch(sample_method, samples_to_submit, batch_dir)
                            new_dirs = new_dirs[size_batch - len(samples_to_submit):]
                        samples_to_submit += [watch_dir / sample_method / prefix_dir / shard_dir / z for z in new_dirs]
            samples_to_submit = process_batch(sample_method, samples_to_submit, batch_dir)

        print("sleeping for 60")
        time.sleep(60)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    argh.dispatch_commands(
        [
            watch,
        ]
    )
