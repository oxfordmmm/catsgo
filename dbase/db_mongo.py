import pymongo
from dbase.db_mongo_dir_watcher import DirWatcher

# Needed for testing, so that they can be mocked
def get_mongo_client():
    return pymongo.MongoClient("mongodb://localhost:27017/")


class DBMongo:
    def __init__(self):
        self._myclient = get_mongo_client()
        self._dir_watcher = None

    @property
    def dir_watcher(self):
        if self._dir_watcher is None:
            self._dir_watcher = DirWatcher(self)
        return self._dir_watcher
