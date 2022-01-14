import time


class Metadata:
    # this is needed so we can monkey patch for testing
    def _get_collection(self):
        return self._parent._mydb["metadata"]

    def __init__(self, dirwatcher):
        self._parent = dirwatcher
        self._metadata_col = self._get_collection()

    def update_one(
        self, new_dir, run_uuid, apex_batch, apex_samples, submitted_metadata
    ):
        self._metadata_col.update_one(
            {"catsup_uuid": new_dir},
            {
                "$set": {
                    "run_uuid": run_uuid,
                    "added_time": str(int(time.time())),
                    "apex_batch": apex_batch,
                    "apex_samples": apex_samples,
                    "submitted_metadata": submitted_metadata,
                }
            },
            upsert=True,
        )

    def save_sample_data(self, new_run_uuid, sp3_sample_name, sample_data):
        self._metadata_col.update_one(
            {"run_uuid": new_run_uuid},
            {"$push": {"submitted_sample_data": {sp3_sample_name: sample_data}}},
            upsert=True,
        )

    def get_data_for_run(self, new_run_uuid):
        data = self._metadata_col.find_one({"run_uuid": new_run_uuid}, {"_id": 0})
        return data
