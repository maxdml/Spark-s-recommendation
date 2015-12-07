from os import listdir
from os.path import isfile, isdir, join
import sys
import json

from eventlog import *
from btracelog import *

class Info:
    def __init__(self):
        self.app_id = None
        self.conf_id = None
        self.app_name = None
        self.parameter = None

        self.running_time = None
        self.gc_time = None
        self.avg_cpu_usage = None
        self.max_memory = None

    def __repr__(self):
        result = ""
        result += "app_id = " + str(self.app_id) + "\n"
        result += "conf_id = " + str(self.conf_id) + "\n"
        result += "app_name = " + str(self.app_name) + "\n"
        result += "parameter = " + str(self.parameter) + "\n"
        result += "running_time = " + str(self.running_time) + " (ms)\n"
        result += "gc_time = " + str(self.gc_time) + " (ms)\n"
        result += "avg_cpu_usage = " + str(self.avg_cpu_usage) + "\n"
        result += "max_memory = " + str(self.max_memory) + " (MB)"
        return result

    def create_summary_log(self, fname):
        data = {}
        data["app_id"] = self.app_id
        data["conf_id"] = self.conf_id
        data["app_name"] = self.app_name
        data["parameter"] = self.parameter
        data["running_time"] = self.running_time
        data["gc_time"] = self.gc_time
        data["avg_cpu_usage"] = self.avg_cpu_usage
        data["max_memory"] = self.max_memory
        f = open(fname, "w")
        f.write(json.dumps(data, indent=4, separators=(',',': ')))
        f.close()


def main(string):
    result = Info()

    if string[-1] == "/":
        string = string[:-1]
    lst = string.split("/")

    path = "/".join(lst[:-1])  # path to input directory
    if len(lst) > 1:
        path += "/"
    directory = lst[-1]  # input directory

    lst = directory.split("-")
    if len(lst) < 7:
        print "Invalid directory format."
        return

    # parse directory name to get app id, name, and its parameters.
    result.app_id = "-".join(lst[0:3])
    result.conf_id = "-".join(lst[3:6])
    result.app_name = lst[6]
    result.parameter = "-".join(lst[7:])

    pd = path + directory

    for f in listdir(pd):
        if f == directory + ".js":
            print "Result already exists."
            return

    eventlog_fname = ""
    for f in listdir(pd):
        if f.split("-")[0] == "app":
            eventlog_fname = join(pd, f)

    # EventLog
    eventlog = EventLog(eventlog_fname)
    result.running_time = eventlog.app_runtime
    result.gc_time = eventlog.gc_time

    # BtraceLogs from all executors
    execdirs = sorted([ d for d in listdir(pd) if isdir(join(pd,d)) ])
    btracelog_fnames = []
    for d in execdirs:
        for f in listdir(join(pd,d)):
            fname = join(pd,d,f)
            if isfile(fname) and fname.split(".")[-1] == "btrace":
                btracelog_fnames.append(fname)
    btracelogs = []
    for fname in btracelog_fnames:
        btracelogs.append(BtraceLog(fname))

    if len(btracelogs) > 0:
        # Max peak memory among all executors
        result.max_memory = max([btracelog.max_memory for btracelog in btracelogs])

        # Avg CPU usage among all executors
        cpu = [btracelog.avg_cpu_load for btracelog in btracelogs]
        result.avg_cpu_usage = sum(cpu) / len(cpu)
    elif len(btracelogs) == 0:
        print "No BTrace logs exist."

    # Create a json file containing results
    result.create_summary_log(pd + "/" + directory + ".js")
    print result


if __name__ == "__main__":
    main(sys.argv[1])
