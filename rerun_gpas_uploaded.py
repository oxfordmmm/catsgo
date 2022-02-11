#!/usr/bin/env python3

"""rerun_gpas_uploaded.py
   
   This script will find the batches with the Uploaded status in the last 14 days, 
   delete the sp3data.csv if present and try to remove the batch from the
   dirlist and ignore_list of the mongo DB.
"""
import db
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dateutil.parser import isoparse
import pymongo


def remove_from_mongo(watch_dir, new_dir):
    # Setup Mongo DB connection
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["dir_watcher"]
    dirlist = mydb["dirlist"]
    ignore_list = mydb["ignore_list"]
    
    print(f"removing {new_dir} from mongo dirlist ignore_list of {watch_dir}")
    dirlist.update_one(
        {"watch_dir": watch_dir}, {"$pull": {"dirs": new_dir}}, upsert=True
    )
    ignore_list.update_one(
        {"watch_dir": watch_dir}, {"$pull": {"ignore_list": new_dir}}, upsert=True
    )

apex_token = db.get_apex_token()
headers = {"Authorization": f"Bearer {apex_token}"}

# Get organisation to lookup input buckets
orgs = set()
url = "https://portal.dev.gpas.ox.ac.uk/ords/gpasdevpdb1/grsp/sp3/organisations"
response = requests.get(url, headers=headers).json()

org_buckets = dict()
for org in response['items']:
    org_buckets[org['organisationName']] = org['inputBucketName']

url = "https://portal.dev.gpas.ox.ac.uk/ords/gpasdevpdb1/grsp/sp3/batches_by_status/Uploaded"
response = requests.get(url, headers=headers).json()
found_batches = []

# Go through each batch <14 days old with Uploaded status
for batch in response['items']:
    if isoparse(batch['uploadedOn']) > (datetime.now(timezone.utc) - timedelta(days=14)):
        if batch['fileName'] not in found_batches:
            found_batches.append(batch['fileName'])
            batch_path = Path("/data/inputs/s3") / org_buckets[batch['organisationName']] / batch['fileName']

            # Check sp3data.csv is present and delete if so
            if batch_path.exists():
                if (batch_path / "sp3data.csv" ).exists():
                    print(f"{batch['fileName']} does exist and has a sp3data. Deleting")
                    (batch_path / "sp3data.csv" ).unlink()
                else:
                    print(f"{batch['fileName']} does exist")
                remove_from_mongo(str(Path("/data/inputs/s3") / org_buckets[batch['organisationName']]), batch['fileName'])
            else:
                print(f"{batch['fileName']} doesn't exist")

