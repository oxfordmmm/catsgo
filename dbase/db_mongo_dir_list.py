import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class DirList:
    # this is needed so we can monkey patch for testing
    def _get_collection(self):
        return self._parent._mydb["dirlist"]

    def __init__(self, dir_watcher):
        self._parent = dir_watcher
        self._dir_list_col = self._get_collection()

    def get_cached(self, watch_dir):
        """
        get the list of uuids that have already run

        (since the dirs are named after catsup upload uuids)
        """
        ret = self._dir_list_col.find_one({"watch_dir": watch_dir}, {"dirs": 1})
        if not ret:
            return list()
        else:
            return list(ret.get("dirs"))

    def add_to_cached(
        self,
        watch_dir,
        new_dir,
        run_uuid,
        apex_batch,
        apex_samples,
        submitted_metadata,
    ):
        """
        add the uuid to the list of uuids that have been run on sp3

        store some metadata as well
        """

        # print(apex_batch)
        # print(apex_samples)

        apex_batch["id"] = str(apex_batch["id"])  # ugh

        logging.debug(f"adding {new_dir}")
        self._dir_list_col.update_one(
            {"watch_dir": watch_dir}, {"$push": {"dirs": new_dir}}, upsert=True
        )
        logging.info(f"{run_uuid}, {submitted_metadata}, {apex_batch}")
        self._parent.metadata.update_one(
            new_dir, run_uuid, apex_batch, apex_samples, submitted_metadata
        )

    def remove_from_cached(self, watch_dir, new_dir):
        logging.debug(f"removing {new_dir}")
        self._dir_list_col.update_one(
            {"watch_dir": watch_dir}, {"$pull": {"dirs": new_dir}}, upsert=True
        )
