from event.environment import *
from event.job import *
from event.stage import *
from event.task import *

class EventLog:
    def __init__(self, eventlog_fname):
        self.eventlog_fname = eventlog_fname
        self.environment = None
        self.jobs = []

        self.app_runtime = None
        self.gc_time = None

        self.parse()
        self.get_gc_time()



    ####
    # Returns the list of jobs in which each of jobs contains all the
    # stages and tasks that have ran during the job.
    def parse(self):
        last_job = None
        f = open(self.eventlog_fname, "r")
        for line in f.readlines():
            j = json.loads(line)
            if j["Event"] == "SparkListenerEnvironmentUpdate":
                self.environment = Environment(j)
            elif j["Event"] == "SparkListenerApplicationStart":
                self.app_runtime = j["Timestamp"]
            elif j["Event"] == "SparkListenerApplicationEnd":
                self.app_runtime = j["Timestamp"] - self.app_runtime
            elif j["Event"] == "SparkListenerJobStart":
                job = Job(j)
                last_job = job
            elif j["Event"] == "SparkListenerJobEnd":
                assert last_job.job_id == j["Job ID"]
                last_job.add_end(j)
                self.jobs.append(last_job)
            elif j["Event"] == "SparkListenerStageSubmitted":
                stage_id = j["Stage Info"]["Stage ID"]
                stage_attempt_id = j["Stage Info"]["Stage Attempt ID"]
                last_job.stages[(stage_id, stage_attempt_id)] = Stage(j)
            elif j["Event"] == "SparkListenerStageCompleted":
                stage_id = j["Stage Info"]["Stage ID"]
                stage_attempt_id = j["Stage Info"]["Stage Attempt ID"]
                assert last_job.stages.has_key((stage_id, stage_attempt_id))
                last_job.stages[(stage_id, stage_attempt_id)].add_end(j)
            elif j["Event"] == "SparkListenerTaskStart":
                stage_id = j["Stage ID"]
                stage_attempt_id = j["Stage Attempt ID"]
                assert last_job.stages.has_key((stage_id, stage_attempt_id))
                stage = last_job.stages[(stage_id, stage_attempt_id)]
                task_id = j["Task Info"]["Task ID"]
                task_attempt_id = j["Task Info"]["Attempt"]
                stage.tasks[(task_id, task_attempt_id)] = Task(j)
            elif j["Event"] == "SparkListenerTaskEnd":
                stage_id = j["Stage ID"]
                stage_attempt_id = j["Stage Attempt ID"]
                assert last_job.stages.has_key((stage_id, stage_attempt_id))
                stage = last_job.stages[(stage_id, stage_attempt_id)]
                task_id = j["Task Info"]["Task ID"]
                task_attempt_id = j["Task Info"]["Attempt"]
                assert stage.tasks.has_key((task_id, task_attempt_id))
                stage.tasks[(task_id, task_attempt_id)].add_end(j)
        f.close()

    def get_gc_time(self):
        self.gc_time = 0
        for job in self.jobs:
            for stage in job.id_sorted_stages:
                for task in stage.id_sorted_tasks:
                    self.gc_time += task.task_metrics.jvm_gc_time

    def print_info(self):
        temp = ""
        for job in self.jobs:
            temp += "\nJob " + str(job.job_id)
            for stage in job.id_sorted_stages:
                temp += "\n  Stage " + str(stage.stage_id) + "\n    Task [ "
                for task in stage.id_sorted_tasks:
                    temp += str(task.task_id) + ","
                temp = temp[:-1]
                temp += " ]"
        print temp
        print ""
