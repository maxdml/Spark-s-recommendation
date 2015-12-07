import numpy as np

class BtraceLog:
    def __init__(self, btracelog_fname):
        self.btracelog_fname = btracelog_fname
        self.max_memory = None
        self.avg_cpu_load = None

        self.parse()

    def __repr__(self):
        return "(" + str(self.max_memory) + "," + str(self.avg_cpu_load) + ")"

    def parse(self):
        time, memory, cpu = np.loadtxt(self.btracelog_fname, unpack=True, delimiter=",", usecols=(0,3,4))
        self.max_memory = max(memory)
        self.avg_cpu_load = sum(cpu) / len(cpu)
