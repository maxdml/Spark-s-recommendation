import numpy as np

class BtraceLog:
    def __init__(self, btracelog_fname):
        self.btracelog_fname = btracelog_fname
        self.max_memory = None
        self.max_heap = None
        self.max_non_heap = None
        self.avg_process_cpu_load = None
        self.avg_system_cpu_load = None

        self.parse()

    def __repr__(self):
        return "(" + str(self.max_memory) + "," + str(self.avg_cpu_load) + ")"

    def parse(self):
        time, max_heap, max_non_heap, memory, process_cpu, system_cpu = np.loadtxt(self.btracelog_fname, unpack=True, delimiter=",", usecols=(0,1,2,3,4,5))
        self.max_memory = max(memory)
        self.max_heap = max(max_heap)
        self.max_non_heap = max(max_non_heap)
        self.avg_process_cpu_load = sum(process_cpu) / len(process_cpu)
        self.avg_system_cpu_load = sum(system_cpu) / len(system_cpu)
