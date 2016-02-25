#!/usr/bin/python

from os import listdir, mkdir
from os.path import isfile, join
import numpy as np
import time
import sys 
import matplotlib.pyplot as plt

from appInfo import *

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
Generates a bar chart plot representing an application memory usage.
One bar display the storage fraction, average and max memory used.
"""

def genMemoryUsagePlot(indicators, plot_loc):
  fig,ax = plt.subplots()

  xticks = np.arange(len(indicators))

  unzip = zip(*indicators)
  mem_line = unzip[1]
  max_heap_line = unzip[2]
  confs = unzip[0]

  mem_bar = ax.bar(xticks, mem_line, color='red')
  max_heap_bar = ax.bar(xticks, max_heap_line, color='green')

  ax.set_ylabel('Heap efficiency (MB)')
  #ax.set_title('')
  xTickMarks = [conf for conf in confs]
  ax.set_xticks(xticks)
  xtickNames = ax.set_xticklabels(xTickMarks)
  plt.setp(xtickNames, rotation=45, fontsize=10)

  plt.savefig(plot_loc)

def efficiencyScatterPlot(indicators, plot_loc):
  fix,ax = plt.subplots()

  xticks = np.arange(len(indicators))

  unzip = zip(*indicators)
  x = unzip[1]
  y = unzip[2]
  confs = unzip[0]

  scatter = ax.scatter(x, y, color='red', s=20, edgecolor='none')
  ax.set_aspect(1./ax.get_data_ratio())

  ax.set_ylim([0,1])
  ax.set_xlim([0,1])

  for x, y, c  in zip(x, y, confs):
    plt.annotate(c, xy = (x,y))

  plt.savefig(plot_loc)

def main(directory_list):
  fh = open(directory_list, 'r')

  list_path = directory_list.split('/')[:-1]
  plot_dir = '/'.join(list_path) + 'configuration-comparison-plots-' + str(int(time.time())) + '/'
  mkdir(plot_dir)

  applications = []

  for directory in fh.readlines():
    directory = directory.rstrip('\n\r')

    #TODO: make this parser more tolerant to unexpected JS files, placement errors, etc
    for f in listdir(directory):
      if '.js' in f:
        print(f)
        app = appInfo()
        applications.append(app.buildFromJson(join(directory, f)))

    plots = {'running_time'             : 'running time (MS)',
             'gc_time'                  : 'total time spent in GC (MS)',
             'max_heap'                 : 'max heap usage (MB)',
             'avg_process_cpu_load'     : 'average cpu load',
             'avg_heap'                 : 'average heap usage (MB)',
             'tasks_per_second'         : 'tasks per second',
             'avg_process_cpu_variance' : 'average cpu variance',
             'gc_to_rt'                 : 'fraction of time spent in GC (MS)'}

    for plot in plots.keys():
      plot_loc = plot_dir + plot + '.png'

      indicators = []
      for app in applications:
        p_id = app.conf_id
        metric = getattr(app, plot)
        indicators.append((p_id, metric))

      genBarPlot(indicators, 'Configurations', plots[plot], plot_loc)


    memories = []
    efficiencies = []
    for app in applications:
      p_id = getattr(app, 'conf_id')
      mem = int(p_id.split('-')[1]) * 1000
      memories.append((app.conf_id, mem, getattr(app, 'max_heap')))

      mem_efficiency = getattr(app, 'max_heap') / mem
      efficiencies.append((getattr(app, 'conf_id'), mem_efficiency, getattr(app, 'avg_process_cpu_load')))

    genMemoryUsagePlot(memories, plot_dir + 'mem-efficiency.png')
    efficiencyScatterPlot(efficiencies, plot_dir + 'mem-scatter.png')


if __name__ == "__main__":
  main(sys.argv[1])
