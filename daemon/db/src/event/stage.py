import json
import re

##
# Stage class comprises information from both Spark's eventlog and btracelog.
##
class Stage:
    def __init__(self, start):
        # Eventlog Information
        self.stage_id = start["Stage Info"]["Stage ID"]
        self.stage_attempt_id = start["Stage Info"]["Stage Attempt ID"]
        self.parent_ids = start["Stage Info"]["Parent IDs"]
        self.submission_time = None
        self.completion_time = None
        self.rdds = None
        self.tasks = {}
        self.id_sorted_tasks = []

        self.total_bytes_cached = 0L
        self.total_records_read = 0L
        self.total_bytes_read = 0L

    def add_end(self, end):
        assert self.stage_id == end["Stage Info"]["Stage ID"]
        assert self.stage_attempt_id == end["Stage Info"]["Stage Attempt ID"]
        self.submission_time = end["Stage Info"]["Submission Time"]
        self.completion_time = end["Stage Info"]["Completion Time"]
        self.rdds = []
        for r in end["Stage Info"]["RDD Info"]:
            rdd = Stage.RDD(r)
            self.rdds.append(rdd)
        self.get_id_sorted_tasks()

    def get_id_sorted_tasks(self):
        def _task_id_comp(x, y):
            c = int(x.task_id) - int(y.task_id)
            if c < 0: return -1
            elif c > 0: return 1
            else:
                c2 = int(x.task_attempt_id) - int(y.task_attempt_id)
                if c2 < 0: return -1
                elif c2 > 0: return 1
                return 0
        self.id_sorted_tasks = self.tasks.values()
        self.id_sorted_tasks.sort(cmp=_task_id_comp)

    def __repr__(self):
        result = "[Stage " + str(self.stage_id) + "] "
        result += "total_bytes_read: " + self._adjust_size(self.total_bytes_read)
        result += ", total_records_read: " + str(self.total_records_read)
        result += ", totoal_bytes_cached: " + self._adjust_size(self.total_bytes_cached)
        return result

    def _new_repr(self, status):
        match = re.search("]", str(self))
        if match is None:
            return str(self)
        index = match.start()
        return str(self)[:index] + " " + status + str(self)[index:]

    def _adjust_size(self, size):
        l = len(str(size))
        if l <= 3: return str(size) + "(B)"
        elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
        else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"

    def compute_total_data_read_cached(self):
        for task in self.tasks.values():
            if task.input_metrics != None:
                self.total_records_read += task.input_metrics.records_read
                self.total_bytes_read += task.input_metrics.bytes_read
            if len(task.updated_blocks) != 0:
                for block in task.updated_blocks:
                    self.total_bytes_cached += block.memory_size

    class RDD:
        def __init__(self, j):
            self.rdd_id = j["RDD ID"]
            self.name = j["Name"]
            self.storage_level = Stage.StorageLevel(j["Storage Level"])
            self.num_partitions = j["Number of Partitions"]
            self.num_cached_partitions = j["Number of Cached Partitions"]
            self.disk_size = j["Disk Size"]
            self.memory_size = j["Memory Size"]
            self.offheap_size = j["ExternalBlockStore Size"]

    class StorageLevel:
        def __init__(self, j):
            self.use_disk = j["Use Disk"]
            self.use_memory = j["Use Memory"]
            self.use_offheap = j["Use ExternalBlockStore"]
