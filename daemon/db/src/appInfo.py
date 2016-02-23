from decimal import Decimal
import json

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

  def buildFromJson(self, json_fname):
    # We expect the json file to be one unique line
    fh = open(json_fname, 'r')
    for line in fh.readlines():
      data = json.loads(line)

      self.app_id = data['app_id']
      self.conf_id = data['conf_id'] 
      self.app_name = data['app_name']
      self.parameters = data['parameters']

      self.running_time = data['running_time']
      self.gc_time = data['gc_time']
      self.gc_to_rt = data['gc_to_rt']

      self.tasks_per_second = data['tasks_per_second']

      self.avg_system_cpu_load = data['avg_system_cpu_load']
      self.avg_process_cpu_load = data['avg_process_cpu_load']
      self.avg_process_cpu_variance = data['avg_process_cpu_variance']

      self.avg_non_heap = data['avg_non_heap']
      self.avg_heap = data['avg_heap']
      self.avg_memory = data['avg_memory']

      self.max_non_heap = data['max_non_heap']
      self.max_heap = data['max_heap']
      self.max_memory = data['max_memory']


    fh.close()

    return self
