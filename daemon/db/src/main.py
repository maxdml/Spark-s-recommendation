from os import listdir, mkdir
from decimal import Decimal
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
    self.parameters = None

    self.running_time = None
    self.gc_time = None
    self.gc_to_rt = None

    self.tasks_per_second = None

    self.avg_process_cpu_load = None
    self.avg_system_cpu_load = None
    self.avg_process_cpu_variance = None
    self.max_memory = None
    self.max_heap = None
    self.max_non_heap = None
    self.avg_heap = None
    self.avg_non_heap = None
    self.avg_memory = None


  def __repr__(self):
    result = ""
    result += "app_id = " + str(self.app_id) + "\n"
    result += "conf_id = " + str(self.conf_id) + "\n"
    result += "app_name = " + str(self.app_name) + "\n"
    result += "parameters = " + str(self.parameters) + "\n"
    result += "running_time = " + str(self.running_time) + " (ms)\n"
    result += "gc_time = " + str(self.gc_time) + " (ms)\n"
    result += "tasks_per_second = " + str(self.tasks_per_second) + '\n'
    result += "avg_process_cpu_load = " + str(self.avg_process_cpu_load) + "\n"
    result += "avg_system_cpu_load = " + str(self.avg_system_cpu_load) + "\n"
    result += "avg_process_cpu_variance = " + str(self.avg_process_cpu_variance) + "\n"
    result += "max_memory = " + str(self.max_memory) + " (MB)" + "\n"
    result += "max_heap = " + str(self.max_heap) + " (MB)" + "\n"
    result += "max_non_heap = " + str(self.max_non_heap) + " (MB)" + "\n"
    result += "avg_heap = " + str(self.avg_heap) + " (MB)" + "\n"
    result += "avg_non_heap = " + str(self.avg_non_heap) + " (MB)" + "\n"
    result += "avg_memory = " + str(self.avg_memory) + " (MB)"
    return result

  def create_summary_log(self, fname):

    self.gc_to_rt = round(Decimal(self.gc_time) / Decimal(self.running_time), 3)

    data = {}
    data["app_id"] = self.app_id
    data["conf_id"] = self.conf_id
    data["app_name"] = self.app_name
    data["parameters"] = self.parameters
    data["running_time"] = self.running_time
    data["gc_time"] = self.gc_time
    data["gc_to_rt"] = self.gc_to_rt
    data["tasks_per_second"] = self.tasks_per_second
    data["avg_process_cpu_load"] = self.avg_process_cpu_load
    data["avg_system_cpu_load"] = self.avg_process_cpu_load
    data["avg_process_cpu_variance"] = self.avg_process_cpu_variance
    data["max_memory"] = self.max_memory
    data["max_heap"] = self.max_heap
    data["max_non_heap"] = self.max_non_heap
    data["avg_memory"] = self.avg_memory
    data["avg_heap"] = self.avg_heap
    data["avg_non_heap"] = self.avg_non_heap

    f = open(fname, "w")
    f.write(json.dumps(data, separators=(',',': ')))
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

  plt.plot(x, y, label = indicator, color = 'black')
  plt.xlabel(xlabel)
  plt.ylabel(ylabel)

  plt.savefig(plot_loc + plot_name)
  fig.clear()

"""
Generates a bar chart for an executor
:param indicators: a list of tuples (executor_id, metric_value)
:param xlabel text for abscissa
:param ylabel text for ordinate
:param plot_loc where to save the plot
:return None
"""

def genBarPlot(indicators, xlabel, ylabel, plot_loc):
  fig,ax = plt.subplots()

  ind = np.arange(len(indicators))
  values = [i[1] for i in indicators]
  executors = [i[0] for i in indicators]

  rects1 = ax.bar(ind, values, color = 'black')

  ax.set_ylabel(ylabel)
  ax.set_title(xlabel)
  xTickMarks = [executor for executor in executors]
  ax.set_xticks(ind)
  xtickNames = ax.set_xticklabels(xTickMarks)
  plt.setp(xtickNames, rotation=45, fontsize=10)

  plt.savefig(plot_loc)



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
  app_info.parameters = "-".join(lst[7:])

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
    else:
      print('app already mined')
      return

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
      else:
        print('app already mined')
        return

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

      app_plots_dir = app_info.app_id + '-plots/'
      if (not isdir(app_plots_dir)):
        mkdir(app_plots_dir)
      else:
        print('app already mined')
        return

      # We could/should have one data structure for each holding every metric

      plot_name = app_plots_dir + 'max-heap-usage.png'
      avg_heaps = [(btracelog.executor_id, btracelog.max_heap) for btracelog in btracelogs]
      genBarPlot(avg_heaps, 'Executor id', 'Max Heap used (MB)',  plot_name)

      plot_name = app_plots_dir + 'avg-heap-usage.png'
      avg_heaps = [(btracelog.executor_id, btracelog.avg_heap) for btracelog in btracelogs]
      genBarPlot(avg_heaps, 'Executor id', 'Average Heap usage (MB)',  plot_name)

      plot_name = app_plots_dir + 'avg-non-heap-usage.png'
      avg_heaps = [(btracelog.executor_id, btracelog.avg_non_heap) for btracelog in btracelogs]
      genBarPlot(avg_heaps, 'Executor id', 'Average non Heap usage (MB)',  plot_name)

      plot_name = app_plots_dir + 'avg-memory-usage.png'
      avg_heaps = [(btracelog.executor_id, btracelog.avg_memory) for btracelog in btracelogs]
      genBarPlot(avg_heaps, 'Executor id', 'Average process memory usage (MB)',  plot_name)

      plot_name = app_plots_dir + 'avg-process-cpu-fraction.png'
      avg_heaps = [(btracelog.executor_id, btracelog.avg_process_cpu_load) for btracelog in btracelogs]
      genBarPlot(avg_heaps, 'Executor id', 'Average process cpu load',  plot_name)

      plot_name = app_plots_dir + 'avg-system-cpu-fraction.png'
      avg_heaps = [(btracelog.executor_id, btracelog.avg_system_cpu_load) for btracelog in btracelogs]
      genBarPlot(avg_heaps, 'Executor id', 'Average system cpu load',  plot_name)

      plot_name = app_plots_dir + 'process-cpu-variance.png'
      avg_heaps = [(btracelog.executor_id, btracelog.process_cpu_variance) for btracelog in btracelogs]
      genBarPlot(avg_heaps, 'Executor id', 'Process cpu variance',  plot_name)

    elif len(btracelogs) == 0:
      print "No BTrace logs exist."


  elif (mode == 'global'):
    ################################################################
    # Get application wide metrics (average accross all executors) #
    ################################################################
    for f in listdir(pd):
      if f == directory + "-global-log.js":
        print "Result already exists."
        return

    # BtraceLogs and GC logs from all executors
    btracelogs, gclogs = findLogs(pd)

    if len(btracelogs) > 0:
      # Avg CPU usage among all executors
      process_cpu = [btracelog.avg_process_cpu_load for btracelog in btracelogs]
      app_info.avg_process_cpu_load = sum(process_cpu) / len(process_cpu)

      system_cpu = [btracelog.avg_system_cpu_load for btracelog in btracelogs]
      app_info.avg_system_cpu_load = sum(system_cpu) / len(system_cpu)

      # Avg CPU variance among all executors
      process_cpu_variance = [btracelog.process_cpu_variance for btracelog in btracelogs]
      app_info.avg_process_cpu_variance = sum(process_cpu_variance) / len(process_cpu_variance)

      # Avg Heap usage among all executors
      avg_heap = [btracelog.avg_heap for btracelog in btracelogs]
      app_info.avg_heap = sum(avg_heap) / len(avg_heap)

      # Avg non Heap usage among all executors
      avg_non_heap = [btracelog.avg_non_heap for btracelog in btracelogs]
      app_info.avg_non_heap = sum(avg_non_heap) / len(avg_non_heap)

      # Avg Heap usage among all executors
      avg_memory = [btracelog.avg_memory for btracelog in btracelogs]
      app_info.avg_memory = sum(avg_memory) / len(avg_memory)

      # Max peak memory (heap + non heap) reached by any executor
      app_info.max_memory = max([btracelog.max_memory for btracelog in btracelogs])

      # Max Heap usage reached by any executor
      app_info.max_heap = max([btracelog.max_heap for btracelog in btracelogs])

      # Max non heap usage reached by any executor
      app_info.max_non_heap = max([btracelog.max_non_heap for btracelog in btracelogs])

      # Create a json file containing results
      app_info.create_summary_log(pd + "/" + directory + "-global-log.js")

      #print app_info

    elif len(btracelogs) == 0:
      print "No BTrace logs exist."


if __name__ == "__main__":
  main(sys.argv[1], sys.argv[2])
