#!/usr/bin/python
import sys
import io
import argparse
import json
import os

def gather(results_dir,
           combination,
           spark_conf,
           spark_logs,
           master_log):

  driver_btrace = '/tmp/' + combination + '-Driver'
  driver_gc     = '/tmp/' + combination + '-DriverGc'
  slaves        = spark_conf + 'slaves'
  master_log    = spark_logs + master_log

  #TODO: very ugly way to get the event log w/ a lot of assumptions that WILL eventually break (single user, etc)
  cmd = 'ls -t ' + spark_logs + 'app* | head -n 1'
  event_log = os.path.split(os.popen(cmd).read().rstrip('\n'))[1]
  app_id    = event_log.split('-', 1)[1]
  run_dir   = results_dir + app_id + '-' + combination

  # Ensure data directory exists 
  if (not os.path.isdir(results_dir)):
    os.mkdir(results_dir)

  # Create dir for the current run
  # TODO: (unlikely) what if there is two same app_id?
  os.mkdir(run_dir)

  # get driver gc logs
  cmd = 'cp ' + driver_gc + ' ' + run_dir
  os.system(cmd)

  # get driver btrace logs
  cmd = 'cp ' + driver_btrace + '* ' + run_dir
  os.system(cmd)

  # get event log
  cmd = 'cp ' + spark_logs + event_log + ' ' + run_dir
  os.system(cmd)

  # get workers logs
  f = open(slaves, 'r')
  #TODO: handle comments in the slaves list
  for slave in f.readlines():
    print(slave)
    
  f.close()

def main():

  description = 'Spark submit interface'

  parser = argparse.ArgumentParser(description=description)
  parser.add_argument('application', help='Name of the application')
  parser.add_argument('executors', help='Number of executors to launch')
  parser.add_argument('memory', help='Amount of memory per executor')
  parser.add_argument('cores', help='Amount of cores per executor')
  parser.add_argument('conf', help='Configuration file')
  parser.add_argument('input', help='Annoyingly, please provide the name of the input file')

  args = parser.parse_args()

  app_name = str(args.application)
  executors = str(args.executors)
  memory = str(args.memory)
  cores = str(args.cores)
  total_cores = str(int(args.executors) * int(args.cores))

  #Get configuration
  conf_file = open(args.conf, 'r')
  conf = {} 

  for line in conf_file.readlines():
    j = json.loads(line)
    conf.update(j) 

  conf_file.close()

  combination = executors + '-' + memory + '-' + cores
  run_file = combination + '-' + app_name
  command = conf['spark_home'] + '/bin/spark-submit --driver-java-options "-javaagent:/opt/btrace/build/btrace-agent.jar=unsafe=true,scriptOutputFile=/tmp/' + run_file + '-Driver,script=/opt/spark-1.5.0-bin-hadoop2.6/DriverProbe.class -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/' + run_file + '-DriverGc -Xmx12048m -Xms12048m" --conf "spark.executor.extraJavaOptions=-javaagent:/opt/btrace/build/btrace-agent.jar=unsafe=true,scriptOutputFile=/tmp/' + run_file + '-Exec,script=/opt/spark-1.5.0-bin-hadoop2.6/ExecutorProbe.class -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --executor-memory ' + memory + 'G' + ' --executor-cores ' + cores + ' --total-executor-cores ' + total_cores + ' ' + conf[app_name]

  print('Executing: ')
  print(command)

  #os.system(command)
  gather(conf['results_dir'], combination, conf['spark_conf'], conf['spark_logs'], conf['master_log'])

main()
