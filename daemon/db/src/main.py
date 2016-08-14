from os import listdir, mkdir
from os.path import isfile, isdir, join
import matplotlib.pyplot as plt
import sys
from eventlog import *
from btracelog import *
from appInfo import *

###################
#UTILITY FUNCTIONS#
###################

"""
Flatten a list of lists and or elements
:param listObject the list to be flattened
:return flat list
"""
def flattenList(listObject):
  if not isinstance(listObject, list):
    return listObject
  else:
    if len(listObject) > 0:
      if isinstance(listObject[0], list):
        return flattenList(listObject[0]) + flattenList(listObject[1:])
      else:
        return [listObject[0]] + flattenList(listObject[1:])
    else:
      return []

"""
Generates a list of btrace and GC log files for the application
:param pd full path to application directory
:return (btracelogs,gclogs) a tuple of btracelog and gclogs objects list
"""

def findLogs(pd):
  execdirs = sorted([ d for d in listdir(pd) if isdir(join(pd,d)) ])

  btracelog_fnames = {}
  gclog_fnames = {}
  btracelogs = {}

  for execdir in execdirs:
    btracelog_fnames[execdir] = []
    btracelogs[execdir] = []
    gclog_fnames[execdir] = []

  for d in execdirs:
    for f in listdir(join(pd,d)):
      fname = join(pd,d,f)
      if isfile(fname) and fname.split(".")[-1] == "btrace":
        btracelog_fnames[d].append(fname)
      elif isfile(fname) and fname.split('-')[0] == 'stdout':
        gclog_fnames[d].append(fname)

  for worker in btracelog_fnames:
    for executor in btracelog_fnames[worker]:
      btracelogs[worker].append(BtraceLog(executor))

  return btracelogs, gclog_fnames

"""
Generates a plot with matplotlib
:param indicator the metric to be plot
:param x list of x values
:param y list of y values
:param xlabel text for abscissa
:param ylabel text for ordinate
:param executor_id ID of the executor we gather data from (None for driver)
:param tasks_events a list of triples (time, task_event, task_id)
:param plots_dir root directory to save the plot
:param ylimit the y axis maximum value
:return None
"""

def genPlot(indicator, x, y, xlabel, ylabel, executor_id, task_events, plots_dir, ylim=None):
  fig,ax = plt.subplots()

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

  if ylim:
    ax.set_ylim(top = ylim)

  starts = [(t,e,i,Y) for t,e,i in task_events for X,Y in zip(x,y) if X == t and e == 'start']
  ends = [(t,e,i,Y) for t,e,i in task_events for X,Y in zip(x,y) if X == t and e == 'end']
  for t,e,i,y in starts:
    plt.plot(t,y, 'o', color='blue')
  for t,e,i,y in ends:
    plt.plot(t,y, 'o', color='green')
    #plt.annotate('task %s %s' % (str(i),e), xy = (t,y))

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

"""
def genMemoryPlot(indicators, ylimit, xlabel, ylabel, plot_loc):
  fig,ax = plt.subplots()

  ind = np.arange(len(indicators))

  unzip = zip(*indicators)
  executors = unzip[0]
  max_heaps = unzip[1]

 # mem = ax.bar(ind, mem, color = 'red')
  max_heap = ax.bar(ind, max_heaps, color = 'green')

  ax.set_ylabel(ylabel)
  ax.set_title(xlabel)
  ax.set_ylim(ylimit)
  xTickMarks = [executor for executor in executors]
  ax.set_xticks(ind)
  xtickNames = ax.set_xticklabels(xTickMarks)
  plt.setp(xtickNames, rotation=45, fontsize=10)

  plt.savefig(plot_loc)

"""
#TODO: get executor/application/global actions out of main

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
  app_info.tasks_per_second = eventlog.tasks_per_second

  execdirs = sorted([ d for d in listdir(pd) if isdir(join(pd,d)) ])

  if (mode == 'executor'):
    # BtraceLogs and GC logs from all executors
    btracelogs, gclogs = findLogs(pd)

    heap_size = int(app_info.conf_id.split('-')[1]) * 1000

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

    genPlot('driver-heap-usage', driver_btrace.time, driver_btrace.heap,
            'Time in MS', 'JVM Heap usage (MB)', None, driver_btrace.tasks, driver_plots_dir)

    genPlot('driver-non-heap-usage', driver_btrace.time, driver_btrace.non_heap,
            'Time in MS', 'JVM non Heap usage (MB)', None, driver_btrace.tasks, driver_plots_dir)

    genPlot('driver-memory-usage', driver_btrace.time, driver_btrace.memory,
            'Time in MS', 'JVM total memory usage (MB)', None, driver_btrace.tasks, driver_plots_dir)

    genPlot('driver-process-cpu-usage', driver_btrace.time, driver_btrace.process_cpu,
            'Time in MS', 'JVM CPU usage fraction', None, driver_btrace.tasks, driver_plots_dir)

    genPlot('driver-system-cpu-usage', driver_btrace.time, driver_btrace.system_cpu,
            'Time in MS', 'System CPU usage fraction', None, driver_btrace.tasks, driver_plots_dir)

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

      for worker in btracelogs:
        for executor in btracelogs[worker]:
          # Heap usage
          genPlot('heap-usage', executor.time, executor.heap,
                  'Time in MS', 'JVM Heap usage in (MB)', executor.executor_id, executor.tasks, plots_dir, heap_size)

          # Non Heap usage
          genPlot('non-heap-usage', executor.time, executor.non_heap,
                  'Time in MS', 'JVM Non Heap usage (MB)', executor.executor_id, executor.tasks, plots_dir)

          # All memory (non heap + heap)
          genPlot('memory-usage', executor.time, executor.memory,
                  'Time in MS', 'JVM total memory usage (MB)', executor.executor_id, executor.tasks, plots_dir)

          # Process cpu
          genPlot('process-cpu-usage', executor.time, executor.process_cpu,
                  'Time in MS', 'JVM CPU usage fraction', executor.executor_id, executor.tasks, plots_dir)

          # System cpu 
          genPlot('system-cpu-usage', executor.time, executor.system_cpu,
                  'Time in MS', 'System CPU usage fraction', executor.executor_id, executor.tasks, plots_dir)

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

      app_infos = { 'process_avg_cpus': [],
                    'process_cpu_variances': [],
                    'avg_heaps': [],
                    'avg_non_heaps': [],
                    'avg_memories': [],
                    'max_heaps': [],
                    'system_avg_cpus': []}

      for worker in btracelogs:
        app_infos['max_heaps'].append(
          [(executor.executor_id, executor.max_heap) for executor in btracelogs[worker]])

        app_infos['avg_heaps'].append(
          [(executor.executor_id, executor.avg_heap) for executor in btracelogs[worker]])

        app_infos['avg_non_heaps'].append(
          [(executor.executor_id, executor.avg_non_heap) for executor in btracelogs[worker]])

        app_infos['avg_memories'].append(
          [(executor.executor_id, executor.avg_memory) for executor in btracelogs[worker]])

        app_infos['process_avg_cpus'].append(
          [(executor.executor_id, executor.avg_process_cpu_load) for executor in btracelogs[worker]])

        app_infos['system_avg_cpus'].append(
          [(executor.executor_id, executor.avg_system_cpu_load) for executor in btracelogs[worker]])

        app_infos['process_cpu_variances'].append(
          [(executor.executor_id, executor.process_cpu_variance) for executor in btracelogs[worker]])


      plot_name = app_plots_dir + 'process-cpu-variance.png'
      genBarPlot(flattenList(app_infos['process_cpu_variances']), 'Executor id', 'Process cpu variance',  plot_name)

      plot_name = app_plots_dir + 'max-heap-usage.png'
      genBarPlot(flattenList(app_infos['max_heaps']), 'Executor id', 'Max Heap used (MB)',  plot_name)

      plot_name = app_plots_dir + 'avg-heap-usage.png'
      genBarPlot(flattenList(app_infos['avg_heaps']), 'Executor id', 'Average Heap usage (MB)',  plot_name)

      plot_name = app_plots_dir + 'avg-non-heap-usage.png'
      genBarPlot(flattenList(app_infos['avg_non_heaps']), 'Executor id', 'Average non Heap usage (MB)',  plot_name)

      plot_name = app_plots_dir + 'avg-memory-usage.png'
      genBarPlot(flattenList(app_infos['avg_memories']), 'Executor id', 'Average process memory usage (MB)',  plot_name)

      plot_name = app_plots_dir + 'avg-process-cpu-fraction.png'
      genBarPlot(flattenList(app_infos['process_avg_cpus']), 'Executor id', 'Average process cpu load',  plot_name)

      plot_name = app_plots_dir + 'avg-system-cpu-fraction.png'
      genBarPlot(flattenList(app_infos['system_avg_cpus']), 'Executor id', 'Average system cpu load',  plot_name)

      """
      plot_name = app_plots_dir + 'memory-efficiency.png'
      max_heaps = [(btracelog.executor_id, btracelog.max_heap) for btracelog in btracelogs]
      max_heap = int(app_info.conf_id.split('-')[1]) * 1000
      genMemoryPlot(max_heaps, max_heap, 'Executor id', 'Executor Memory efficiency', plot_name)
      """

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

    # BtraceLogs and GC logs from all workers and their executor(s)
    btracelogs, gclogs = findLogs(pd)
    app_infos = { 'worker_process_cpus': [],
                  'process_cpu_variances': [],
                  'avg_heaps': [],
                  'avg_non_heaps': [],
                  'avg_memories': [],
                  'max_memories': [],
                  'max_heaps': [],
                  'max_non_heaps': [],
                  'system_cpus': [],
                }

    if len(btracelogs) > 0:
      # Avg CPU usage for each worker
      for worker in btracelogs:
        app_infos['worker_process_cpus'].append(
          sum([btracelog.avg_process_cpu_load for btracelog in btracelogs[worker]]))

        app_infos['process_cpu_variances'].append(
          [btracelog.process_cpu_variance for btracelog in btracelogs[worker]])

        app_infos['avg_heaps'].append(
          [btracelog.avg_heap for btracelog in btracelogs[worker]])

        app_infos['avg_non_heaps'].append(
          [btracelog.avg_non_heap for btracelog in btracelogs[worker]])

        app_infos['avg_memories'].append(
          [btracelog.avg_memory for btracelog in btracelogs[worker]])

        app_infos['max_memories'].append(
          max([btracelog.max_memory for btracelog in btracelogs[worker]]))

        app_infos['max_heaps'].append(
          max([btracelog.max_heap for btracelog in btracelogs[worker]]))

        app_infos['max_non_heaps'].append(
          max([btracelog.max_non_heap for btracelog in btracelogs[worker]]))


      # we probably want only a system view from a single executor on the worker, not all of them?
      #system_cpu = [btracelog.avg_system_cpu_load for btracelog in btracelogs]
      #app_info.avg_system_cpu_load = sum(system_cpu) / len(system_cpu)

      # Avg CPU usage among all executors
      app_info.avg_process_cpu_load = \
        sum(app_infos['worker_process_cpus']) / len(app_infos['worker_process_cpus'])

      # Avg CPU variance among all executors
      flat_process_cpu_variance = flattenList(app_infos['process_cpu_variances'])
      app_info.avg_process_cpu_variance = \
        sum(flat_process_cpu_variance) / len(flat_process_cpu_variance)

      # Avg Heap usage among all executors
      flat_avg_heaps = flattenList(app_infos['avg_heaps'])
      app_info.avg_heap = sum(flat_avg_heaps) / len(flat_avg_heaps)

      # Avg non Heap usage among all executors
      flat_avg_non_heaps = flattenList(app_infos['avg_non_heaps'])
      app_info.avg_non_heap = sum(flat_avg_non_heaps) / len(flat_avg_non_heaps)

      # Avg Heap usage among all executors
      flat_avg_memories = flattenList(app_infos['avg_memories'])
      app_info.avg_memory = sum(flat_avg_memories) / len(flat_avg_memories)

      # Max peak memory (heap + non heap) reached by any executor
      app_info.max_memory = max(app_infos['max_memories'])

      # Max Heap usage reached by any executor
      app_info.max_heap = max(app_infos['max_heaps'])

      # Max non heap usage reached by any executor
      app_info.max_non_heap = max(app_infos['max_non_heaps'])

      # Create a json file containing results
      app_info.create_summary_log(pd + "/" + directory + "-global-log.js")

      #print app_info

    elif len(btracelogs) == 0:
      print "No BTrace logs exist."


if __name__ == "__main__":
  main(sys.argv[1], sys.argv[2])
