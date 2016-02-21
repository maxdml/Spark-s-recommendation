import numpy as np

class BtraceLog:
  def __init__(self, btracelog_fname):
    self.btracelog_fname = btracelog_fname
    self.max_memory = None
    self.avg_memory_usage = None
    self.max_heap = None
    self.avg_heap_usage = None
    self.max_non_heap = None
    self.avg_non_heap_usage = None
    self.avg_process_cpu_load = None
    self.avg_system_cpu_load = None
    self.heap = None
    self.non_heap = None
    self.memory = None
    self.process_cpu = None

    self.executor_id = self.btracelog_fname.split('@')[1].split('.')[0]
    self.executor_id += "-"
    self.executor_id += self.btracelog_fname.split('@')[0].split('-')[-1]

    self.parse()

  def __repr__(self):
    return "(" + str(self.max_memory) + "," + str(self.avg_process_cpu_load) + ")"

  def parse(self):
    self.time, self.heap, self.non_heap, self.memory, self.process_cpu, self.system_cpu = np.loadtxt(self.btracelog_fname, unpack=True, delimiter=",", usecols=(0,1,2,3,4,5))
    self.max_memory = max(self.memory)
    self.max_heap = max(self.heap)
    self.max_non_heap = max(self.non_heap)
    self.avg_heap_usage = sum(self.heap) / len(self.heap)
    self.avg_non_heap_usage = sum(self.non_heap) / len(self.non_heap)
    self.avg_memory_usage = sum(self.memory) / len(self.memory)
    self.avg_process_cpu_load = sum(self.process_cpu) / len(self.process_cpu)
    self.avg_system_cpu_load = sum(self.system_cpu) / len(self.system_cpu)
