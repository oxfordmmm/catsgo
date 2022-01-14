class IgnoreList:
    # this is needed so we can monkey patch for testing
    def _get_collection(self):
        return self._parent._mydb["ignore_list"]

    def __init__(self, dirwatcher):
        self._parent = dirwatcher
        self._ignore_list_col = self._get_collection()

    def get(self, watch_dir):
        r = self._ignore_list_col.find_one({"watch_dir": watch_dir}, {"ignore_list": 1})
        if r:
            return r.get("ignore_list", list())
        else:
            return list()

    def add(self, watch_dir, submission_uuid):
        self._ignore_list_col.update_one(
            {"watch_dir": watch_dir},
            {"$push": {"ignore_list": submission_uuid}},
            upsert=True,
        )
