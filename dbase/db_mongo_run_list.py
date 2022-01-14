class RunList:
    # this is needed so we can monkey patch for testing
    def _get_collection(self):
        return self._parent._mydb["runlist"]

    def __init__(self, dirwatcher):
        self._parent = dirwatcher
        self._run_list_col = self._get_collection()

    def get_submitted(self, pipeline_name):
        ret = self._run_list_col.find_one(
            {"pipeline_name": pipeline_name}, {"finished_uuids": 1}
        )
        if not ret:
            return list()
        else:
            return list(ret.get("finished_uuids", list()))

    def add_to_submitted(self, pipeline_name, new_run_uuid):
        self._run_list_col.update_one(
            {"pipeline_name": pipeline_name},
            {"$push": {"finished_uuids": new_run_uuid}},
            upsert=True,
        )
