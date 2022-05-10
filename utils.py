import json
import logging
import requests
import gzip
import csv 
from pathlib import Path

def load_config(config_file):
    with open(config_file) as f:
        cfg = json.loads(f.read())
    return cfg

def load_oracle_config(config_file):
    with open(config_file) as f:
        cfg = json.loads(f.read())
    return cfg["oracle_rest"]

def get_ena_metadata(sample):
    url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?accession={sample}&fields=host%2Cscientific_name%2Cfirst_public%2Ccollection_date%2Cinstrument_platform%2Cinstrument_model%2Ccountry%2Cfastq_md5&format=json&limit=10&result=read_run"
    logging.info(f"getting ena metadata for {sample}")
    response = requests.get(url, timeout=10)
    try:
        j = response.json()
        if len(j) == 1:
            return j[0]
        else:
            logging.error(f"requested more than a single sample: accession {sample}, json {j}")
    except:
        logging.error(f"empty response from host: {url}")
        return None

def get_ena_metadata_from_csv(sample_name, watch_dir):
    logging.info(f"getting ena metadata for {sample_name}")
    with gzip.open(Path(watch_dir) / "batch0-validated.csv.gz", mode="rt") as f:
        reader = csv.DictReader(f)
        ena_rec = [record for record in reader if record["sample_name"] == sample_name]
        if ena_rec:
            return ena_rec[0]
        else:
            return None

def is_number(string):
    
    # check for empty strings
    if not string.strip():
        return False

    try:
        float(string)
        return True
    except :
        return False
