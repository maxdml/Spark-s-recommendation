import json
import re

##
# Job class comprises information from both Spark's eventlog and btracelog.
##
class Job:
    def __init__(self, start):
        # Eventlog Information
        self.job_id = start["Job ID"]
        self.submission_time = start["Submission Time"]
        self.completion_time = None
        self.job_result = None
        self.stages = {}
        self.id_sorted_stages = []

    def add_end(self, end):
        assert self.job_id == end["Job ID"]
        self.completion_time = end["Completion Time"]
        self.job_result = end["Job Result"]["Result"]
        self.get_id_sorted_stages()

    def get_id_sorted_stages(self):
        def _stage_id_comp(x, y):
            c = int(x.stage_id) - int(y.stage_id)
            if c < 0: return -1
            elif c > 0: return 1
            else: 
                c2 = int(x.stage_attempt_id) - int(y.stage_attempt_id)
                if c2 < 0: return -1
                elif c2 > 0: return 1
                return 0
        self.id_sorted_stages = self.stages.values()
        self.id_sorted_stages.sort(cmp=_stage_id_comp)

    def __repr__(self):
        return "[Job " + str(self.job_id) + "]"

    def _new_repr(self, status):
        match = re.search("]", str(self))
        if match is None:
            return str(self)
        index = match.start()
        return str(self)[:index] + " " + status + str(self)[index:]
