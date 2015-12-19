#!/usr/bin/python
import sys
import io
import argparse
import json
import os

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

os.system(command)
