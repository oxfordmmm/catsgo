import pytest
import pytest_check as check
import mongomock
from dbase.db_mongo import DBMongo
from dbase.db_mongo_dir_watcher import DirWatcher
from dbase.db_mongo_dir_list import DirList
from dbase.db_mongo_metadata import Metadata
from dbase.db_mongo_ignore_list import IgnoreList


@pytest.fixture(autouse=True)
def patch_mongo(monkeypatch, mongodb):

    # def fake_mongo():
    #     return mongodb
    # monkeypatch.setattr("db_mongo.get_mongo_client", fake_mongo)

    # monkey patch the dirlist collection
    def fake_mongo_dirlist(self):
        return mongodb.dirlist

    monkeypatch.setattr(DirList, "_get_collection", fake_mongo_dirlist)

    # monkey patch the metadata collection
    def fake_mongo_metadata(self):
        return mongodb.metadata

    monkeypatch.setattr(Metadata, "_get_collection", fake_mongo_metadata)

    # monkey patch the ignorelist collection
    def fake_mongo_ignorelist(self):
        return mongodb.ignore_list

    monkeypatch.setattr(IgnoreList, "_get_collection", fake_mongo_ignorelist)


def test_get_cached(mongodb):
    db = DBMongo()
    dirlist = db.dir_watcher.dir_list.get_cached("/data/inputs/s3/oracle-test")
    # check that get_cached is returning the dirs for oracle-test that we expect
    check.equal(
        dirlist,
        mongodb.dirlist.find_one({"watch_dir": "/data/inputs/s3/oracle-test"}).get(
            "dirs"
        ),
    )


def test_add_to_cached(mongodb):
    db = DBMongo()
    watch_dir = "/data/inputs/s3/oracle-test"
    new_dir = "new_dir_testing"
    run_uuid = ""
    apex_batch = {"status": "success", "id": "273397425188863611944609920003674363934"}
    apex_samples = {
        "samples": [
            {
                "id": "273397425188864820870429534632849070110",
                "name": "644796d3-a507-436b-ad0c-a8aa832d8679",
                "fileName": "644796d3-a507-436b-ad0c-a8aa832d8679",
                "filePath": "644796d3-a507-436b-ad0c-a8aa832d8679",
                "status": "Uploaded",
                "uploadedOn": "2021-10-06T11:27:18.110Z",
                "errorMsg": None,
            }
        ]
    }
    submitted_metadata = {
        "batch": {
            "samples": [
                {
                    "name": "644796d3-a507-436b-ad0c-a8aa832d8679",
                    "host": "Homo sapiens",
                    "collectionDate": "2021-10-06T11:27:18.110Z",
                    "country": "United Kingdom",
                    "fileName": "644796d3-a507-436b-ad0c-a8aa832d8679",
                    "specimenOrganism": "SARS-CoV-2",
                    "specimenSource": "Swab",
                    "status": "Uploaded",
                    "comments": "",
                    "sampleDetails3": "",
                    "submissionTitle": "test",
                    "submissionDescription": "test",
                    "instrument": {
                        "platform": "Nanopore",
                        "model": "Nanopore GridION",
                        "flowcell": "96",
                    },
                    "seReads": [
                        {
                            "uri": "",
                            "sp3_filepath": "/data/inputs/s3/rCgxZDvfQCNzMAQMVzBwhlOxgfWKlhzu/9d302452-d5b5-45c7-ab0c-3c7ad24b84a6/644796d3-a507-436b-ad0c-a8aa832d8679_C1.fastq.gz",
                            "md5": "f19b763866632ca024dfd540d639e147",
                        }
                    ],
                    "peReads": [],
                }
            ],
            "fileName": "9d302452-d5b5-45c7-ab0c-3c7ad24b84a6",
            "bucketName": "rCgxZDvfQCNzMAQMVzBwhlOxgfWKlhzu",
            "uploadedOn": "2021-10-06T11:27:18.110Z",
            "uploadedBy": "oliver.bannister@ouh.nhs.uk",
            "organisation": "GPAS",
            "site": "",
            "errorMsg": "",
        }
    }

    dirlist_dir_count = len(
        mongodb.dirlist.find_one({"watch_dir": watch_dir}).get("dirs")
    )

    db.dir_watcher.dir_list.add_to_cached(
        watch_dir, new_dir, run_uuid, apex_batch, apex_samples, submitted_metadata
    )

    dirs_after = mongodb.dirlist.find_one({"watch_dir": watch_dir}).get("dirs")

    # check that an entry has been added to the dirs
    check.equal(len(dirs_after), dirlist_dir_count + 1)
    # check that the new_dir is in the list of dirs
    dirlist = mongodb.dirlist.find_one(
        {"watch_dir": "/data/inputs/s3/oracle-test"}
    ).get("dirs")
    check.is_in(new_dir, dirlist)
