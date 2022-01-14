from dbase.db_mongo_dir_list import DirList
from dbase.db_mongo_metadata import Metadata
from dbase.db_mongo_ignore_list import IgnoreList
from dbase.db_mongo_run_list import RunList


class DirWatcher:
    def __init__(self, dbmongo):
        self._parent = dbmongo
        self._mydb = self._parent._myclient["dir_watcher"]
        self._dir_list = None
        self._metadata = None
        self._ignore_list = None
        self._run_list = None

    @property
    def dir_list(self):
        if self._dir_list is None:
            self._dir_list = DirList(self)
        return self._dir_list

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = Metadata(self)
        return self._metadata

    @property
    def ignore_list(self):
        if self._ignore_list is None:
            self._ignore_list = IgnoreList(self)
        return self._ignore_list

    @property
    def run_list(self):
        if self._run_list is None:
            self._run_list = RunList(self)
        return self._run_list
