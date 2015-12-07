import json
import re

##
# Task class comprises information from both Spark's eventlog and btracelog.
##
class Task:
    def __init__(self, start):
        # Eventlog Information
        self.stage_id = start["Stage ID"]
        self.stage_attempt_id = start["Stage Attempt ID"]
        self.task_id = start["Task Info"]["Task ID"]
        self.task_attempt_id = start["Task Info"]["Attempt"]
        self.partition_index = start["Task Info"]["Index"]
        self.executor_id = start["Task Info"]["Executor ID"]
        self.host = start["Task Info"]["Host"]

        self.task_metrics = None
        self.launch_time = None
        self.finish_time = None

    def add_end(self, end):
        assert self.stage_id == end["Stage ID"]
        assert self.stage_attempt_id == end["Stage Attempt ID"]
        assert self.task_id == end["Task Info"]["Task ID"]
        assert self.task_attempt_id == end["Task Info"]["Attempt"]
        self.task_metrics = self.TaskMetrics(end["Task Metrics"])
        self.launch_time = end["Task Info"]["Launch Time"]
        self.finish_time = end["Task Info"]["Finish Time"]

    def __repr__(self):
        return "[Task " + str(self.task_id) + "]"

    def _new_repr(self, status):
        match = re.search("]", str(self))
        if match is None:
            return str(self)
        index = match.start()
        return str(self)[:index] + " " + status + str(self)[index:]


    class TaskMetrics:
        def __init__(self, j):
            self.hostname = j["Host Name"]
            self.executor_deserialize_time = j["Executor Deserialize Time"]
            self.executor_run_time = j["Executor Run Time"]
            self.result_size = j["Result Size"]
            self.jvm_gc_time = j["JVM GC Time"]
            self.result_serialization_time = j["Result Serialization Time"]
            self.memory_bytes_spilled = j["Memory Bytes Spilled"]
            self.disk_bytes_spilled = j["Disk Bytes Spilled"]

            self.input_metrics = None
            self.output_metrics = None
            self.shuffle_write_metrics = None
            self.shuffle_read_metrics = None
            self.updated_blocks = None

            if "Input Metrics" in j:
                self.input_metrics = Task.InputMetrics(j["Input Metrics"])
            if "Output Metrics" in j:
                self.output_metrics = Task.OutputMetrics(j["Output Metrics"])
            if "Shuffle Write Metrics" in j:
                self.shuffle_write_metrics = Task.ShuffleWriteMetrics(j["Shuffle Write Metrics"])
            if "Shuffle Read Metrics" in j:
                self.shuffle_read_metrics = Task.ShuffleReadMetrics(j["Shuffle Read Metrics"])
            self.updated_blocks = []
            if "Updated Blocks" in j:
                for b in j["Updated Blocks"]:
                    block = Task.Block(b)
                    self.updated_blocks.append(block)

    class InputMetrics:
        def __init__(self, j):
            self.method = j["Data Read Method"]
            self.bytes_read = j["Bytes Read"]
            self.records_read = j["Records Read"]
        def __repr__(self):
            result  = "bytes_read: " + _adjust_size(self.bytes_read)
            result += ", records_read: " + str(self.records_read)
            return result

    class OutputMetrics:
        def __init__(self, j):
            self.method = j["Data Write Method"]
            self.bytes_written = j["Bytes Written"]
            self.records_written = j["Records Written"]
        def __repr__(self):
            result  = "bytes_written: " + _adjust_size(self.bytes_written)
            result += ", records_written: " + str(self.records_written)
            return result

    class ShuffleWriteMetrics:
        def __init__(self, j):
            self.shuffle_bytes_written = j["Shuffle Bytes Written"]
            self.shuffle_write_time = j["Shuffle Write Time"]
            self.shuffle_records_written = j["Shuffle Records Written"]
        def __repr__(self):
            result = "shuffle_bytes_written: " + _adjust_size(self.shuffle_bytes_written)
            result += ", shuffle_records_written: " + str(self.shuffle_records_written)
            return result

    class ShuffleReadMetrics:
        def __init__(self, j):
            self.remote_blocks_fetched = j["Remote Blocks Fetched"]
            self.local_blocks_fetched = j["Local Blocks Fetched"]
            self.fetch_wait_time = j["Fetch Wait Time"]
            self.remote_bytes_read = j["Remote Bytes Read"]
            self.local_bytes_read = j["Local Bytes Read"]
            self.total_records_read = j["Total Records Read"]
        def __repr__(self):
            result = "remote_bytes_read: " + _adjust_size(self.remote_bytes_read)
            result += ", local_bytes_read: " + _adjust_size(self.local_bytes_read)
            result += ", total_records_read: " + str(self.total_records_read)
            return result

    class Block:
        def __init__(self, j):
            self.block_id = j["Block ID"]  # "rdd_2_0" or "broadcast_1"
            self.storage_level = Task.StorageLevel(j["Status"]["Storage Level"])
            self.disk_size = j["Status"]["Disk Size"]
            self.memory_size = j["Status"]["Memory Size"]
            self.offheap_size = j["Status"]["ExternalBlockStore Size"]

    class StorageLevel:
        def __init__(self, j):
            self.use_disk = j["Use Disk"]
            self.use_memory = j["Use Memory"]
            self.use_offheap = j["Use ExternalBlockStore"]

def _adjust_size(size):
    l = len(str(size))
    if l <= 3: return str(size) + "(B)"
    elif l <= 6: return str( round(size / 1024.0, 2) ) + "(KB)"
    else: return str( round(size / 1024.0 / 1024.0, 2) ) + "(MB)"
