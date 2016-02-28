import numpy as np

class BtraceLog:
  def __init__(self, btracelog_fname):
    self.btracelog_fname = btracelog_fname

    self.time = []
    self.heap = []
    self.non_heap = []
    self.memory = []
    self.process_cpu = []
    self.system_cpu = []
    self.tasks = []

    self.max_memory = None
    self.avg_memory = None
    self.max_heap = None
    self.avg_heap = None
    self.max_non_heap = None
    self.avg_non_heap = None
    self.avg_process_cpu_load = None
    self.avg_system_cpu_load = None
    self.process_cpu_variance = None

    self.executor_id = self.btracelog_fname.split('@')[1].split('.')[0]
    self.executor_id += "-"
    self.executor_id += self.btracelog_fname.split('@')[0].split('-')[-1]

    self.parse()
    self.computeProcessCpuVariance()

  def __repr__(self):
    return "(" + str(self.max_memory) + "," + str(self.avg_process_cpu) + ")"

  def parse(self):

    fh = open(self.btracelog_fname)
    for line in fh.readlines():
      line    = line.rstrip('\n\r')
      columns = line.split(',')

      self.time.append(int(columns[0]))
      self.heap.append(float(columns[1]))
      self.non_heap.append(float(columns[2]))
      self.memory.append(float(columns[3]))
      self.process_cpu.append(float(columns[4]))
      self.system_cpu.append(float(columns[5]))
      try:
        if (columns[6] and columns[6] == 'task'):
          event_time = int(columns[0])
          task_event = columns[7]
          task_id    = int(columns[8])
          self.tasks.append((event_time, task_event, task_id))
      except IndexError:
        e = 'Pass your way, citizen'

    self.max_memory = max(self.memory)
    self.avg_memory = sum(self.memory) / len(self.memory)

    self.max_heap = max(self.heap)
    self.avg_heap = sum(self.heap) / len(self.heap)

    self.max_non_heap = max(self.non_heap)
    self.avg_non_heap = sum(self.non_heap) / len(self.non_heap)

    self.avg_process_cpu_load = sum(self.process_cpu) / len(self.process_cpu)
    self.avg_system_cpu_load = sum(self.system_cpu) / len(self.system_cpu)

  def computeProcessCpuVariance(self):
    deviations = [(cpu_load - self.avg_process_cpu_load) ** 2 for cpu_load in self.process_cpu]
    self.process_cpu_variance = sum(deviations) / len(deviations)
