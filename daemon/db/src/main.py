from os import listdir, mkdir
from os.path import isfile, isdir, join
import matplotlib.pyplot as plt
import sys
import json
from eventlog import *
from btracelog import *

class appInfo:
  def __init__(self):
    self.app_id = None
    self.conf_id = None
    self.app_name = None
    self.parameter = None

    self.running_time = None
    self.gc_time = None
    self.avg_cpu_usage = None
    self.max_memory = None
    self.avg_heap_usage = None

  def __repr__(self):
    result = ""
    result += "app_id = " + str(self.app_id) + "\n"
    result += "conf_id = " + str(self.conf_id) + "\n"
    result += "app_name = " + str(self.app_name) + "\n"
    result += "parameter = " + str(self.parameter) + "\n"
    result += "running_time = " + str(self.running_time) + " (ms)\n"
    result += "gc_time = " + str(self.gc_time) + " (ms)\n"
    result += "avg_process_cpu_usage = " + str(self.avg_process_cpu_usage) + "\n"
    result += "max_memory = " + str(self.max_memory) + " (MB)" + "\n"
    result += "avg_heap_usage = " + str(self.avg_heap_usage) + " (MB)"
    return result

  def create_summary_log(self, fname):
    data = {}
    data["app_id"] = self.app_id
    data["conf_id"] = self.conf_id
    data["app_name"] = self.app_name
    data["parameter"] = self.parameter
    data["running_time"] = self.running_time
    data["gc_time"] = self.gc_time
    data["avg_process_cpu_usage"] = self.avg_process_cpu_usage
    data["max_memory"] = self.max_memory
    f = open(fname, "w")
    f.write(json.dumps(data, indent=4, separators=(',',': ')))
    f.close()

"""
Generates a list of btrace and GC log files for the application
:param pd full path to application directory
:return (btracelogs,gclogs) a tuple of btracelog and gclogs objects list
"""

def findLogs(pd):
  execdirs = sorted([ d for d in listdir(pd) if isdir(join(pd,d)) ])

  btracelog_fnames = []
  gclog_fnames = []
  for d in execdirs:
    for f in listdir(join(pd,d)):
      fname = join(pd,d,f)
      if isfile(fname) and fname.split(".")[-1] == "btrace":
        btracelog_fnames.append(fname)
      elif isfile(fname) and fname.split('-')[0] == 'stdout':
        gclog_fnames.append(fname)

  btracelogs = []
  for fname in btracelog_fnames:
    btracelogs.append(BtraceLog(fname))

  return btracelogs, gclog_fnames

"""
Generates a plot with matplotlib
:param fig the pyplot figure object
:param indicator the metric to be plot
:param x list of x values
:param y list of y values
:param xlabel text for abscissa
:param ylabel text for ordinate
:param executor_id ID of the executor we gather data from (None for driver)
:param plots_dir root directory to save the plot
:return None
"""

def genPlot(fig, indicator, x, y, xlabel, ylabel, executor_id, plots_dir):
  if executor_id:
    plot_name = indicator + '-' + executor_id + '.png'
  else:
    plot_name = indicator + '-driver.png'

  plot_loc = plots_dir + indicator + '/'

  if (not isdir(plot_loc)):
    mkdir(plot_loc)

  plt.plot(x, y, label = indicator)
  plt.xlabel(xlabel)
  plt.ylabel(ylabel)

  plt.savefig(plot_loc + plot_name)
  fig.clear()

def main(directory, mode):
  app_info = appInfo()

  if directory[-1] == "/":
    directory = directory[:-1]
  lst = directory.split("/")

  path = "/".join(lst[:-1])  # path to input directory
  if len(lst) > 1:
    path += "/"
  directory = lst[-1]  # input directory

  lst = directory.split("-")
  if len(lst) < 7:
    print "Invalid directory format.\n"
    print "Valid is of the form app-ts-id-e-m-c-appName-parameterSpace.\n"
    print "e.g. : app-20151105221648-0044-29-14-5-cc-5m"
    return

  # parse directory name to get app id, name, and its parameters.
  app_info.app_id = "-".join(lst[0:3])
  app_info.conf_id = "-".join(lst[3:6])
  app_info.app_name = lst[6]
  app_info.parameter = "-".join(lst[7:])

  pd = path + directory

  eventlog_fname = ""
  for f in listdir(pd):
    split = f.split('-')
    if split[0] == "app" and len(split) == 3:
      eventlog_fname = join(pd, f)

  # EventLog
  eventlog = EventLog(eventlog_fname)
  app_info.running_time = eventlog.app_runtime
  app_info.gc_time = eventlog.gc_time

  execdirs = sorted([ d for d in listdir(pd) if isdir(join(pd,d)) ])

  if (mode == 'executor'):
    # BtraceLogs and GC logs from all executors
    btracelogs, gclogs = findLogs(pd)

    fig,ax = plt.subplots()

    # Get Driver logs
    driver_btrace = None
    driver_gc = None
    for f in listdir(pd):
      if '.btrace' in f:
        #driver_btrace = BtraceLog(pd + '/' + f)
        driver_btrace = BtraceLog(join(pd,f))
      elif 'DriverGc' in f:
        driver_gc = f

    driver_plots_dir = app_info.app_id + '-driver-plots/'
    if (not isdir(driver_plots_dir)):
      mkdir(driver_plots_dir)

    genPlot(fig, 'driver-heap-usage', driver_btrace.time, driver_btrace.heap,
              'Time in MS', 'JVM Heap usage (MB)', None, driver_plots_dir)

    genPlot(fig, 'driver-non-heap-usage', driver_btrace.time, driver_btrace.non_heap,
              'Time in MS', 'JVM non Heap usage (MB)', None, driver_plots_dir)

    genPlot(fig, 'driver-memory-usage', driver_btrace.time, driver_btrace.memory,
              'Time in MS', 'JVM total memory usage (MB)', None, driver_plots_dir)

    genPlot(fig, 'driver-process-cpu-usage', driver_btrace.time, driver_btrace.process_cpu,
              'Time in MS', 'JVM CPU usage fraction', None, driver_plots_dir)

    genPlot(fig, 'driver-system-cpu-usage', driver_btrace.time, driver_btrace.system_cpu,
              'Time in MS', 'System CPU usage fraction', None, driver_plots_dir)

    if len(btracelogs) > 0:
      ####################################
      # Generate plots for every executor#
      ####################################
      plots_dir = app_info.app_id + '-executor-plots/'
      if (not isdir(plots_dir)):
        mkdir(plots_dir)

      for btracelog in btracelogs:
        # Heap usage
        genPlot(fig, 'heap-usage', btracelog.time, btracelog.heap,
                  'Time in MS', 'JVM Heap usage in (MB)', btracelog.executor_id, plots_dir)

        # Non Heap usage
        genPlot(fig, 'non-heap-usage', btracelog.time, btracelog.non_heap,
                  'Time in MS', 'JVM Non Heap usage (MB)', btracelog.executor_id, plots_dir)

        # All memory (non heap + heap)
        genPlot(fig, 'memory-usage', btracelog.time, btracelog.memory,
                  'Time in MS', 'JVM total memory usage (MB)', btracelog.executor_id, plots_dir)

        # Process cpu
        genPlot(fig, 'process-cpu-usage', btracelog.time, btracelog.process_cpu,
                  'Time in MS', 'JVM CPU usage fraction', btracelog.executor_id, plots_dir)

        # System cpu 
        genPlot(fig, 'system-cpu-usage', btracelog.time, btracelog.system_cpu,
                  'Time in MS', 'System CPU usage fraction', btracelog.executor_id, plots_dir)

    elif len(btracelogs) == 0:
      print "No BTrace logs exist."

  elif (mode == 'application'):
    # BtraceLogs and GC logs from all executors
    btracelogs, gclogs = findLogs(pd)

    if len(btracelogs) > 0:
      ################################
      # Get metric for all executors #
      ################################
      print("salut")
    elif len(btracelogs) == 0:
      print "No BTrace logs exist."


  elif (mode == 'global'):
    ################################################################
    # Get application wide metrics (average accross all executors) #
    ################################################################
    for f in listdir(pd):
      if f == directory + "global-log.js":
        print "Result already exists."
        return

    # BtraceLogs and GC logs from all executors
    btracelogs, gclogs = findLogs(pd)

    if len(btracelogs) > 0:
      # Max peak memory among all executors
      app_info.max_memory = max([btracelog.max_memory for btracelog in btracelogs])

      # Avg CPU usage among all executors
      cpu = [btracelog.avg_process_cpu_load for btracelog in btracelogs]
      app_info.avg_process_cpu_usage = sum(cpu) / len(cpu)

      # Avg Heap usage among all executors
      heap = [btracelog.avg_heap_usage for btracelog in btracelogs]
      app_info.avg_heap_usage = sum(heap) / len(heap)

      # Create a json file containing results
      app_info.create_summary_log(pd + "/" + directory + "global-log.js")

      print app_info

    elif len(btracelogs) == 0:
      print "No BTrace logs exist."


if __name__ == "__main__":
  main(sys.argv[1], sys.argv[2])
