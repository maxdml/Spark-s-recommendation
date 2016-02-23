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
        p_id = app.app_name + '-' + app.conf_id
        metric = getattr(app, plot)
        indicators.append((p_id, metric))

      genBarPlot(indicators, 'Configurations', plots[plot], plot_loc)



if __name__ == "__main__":
  main(sys.argv[1])
