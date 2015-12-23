#!/bin/bash

function usage {
  echo 'usage: ./auto_run_wc.sh -f|--input-file'
  echo 'example: ./auto_run_wc.sh -f=dataset-10g'
}

######################################
# Will do '-n' times:                #
# 1 - pick up a combination          #
# 2 - run the experiment * 3         #
# 3 - save the results               #
# 4 - move them in a backup directory#
######################################

if [ $# -lt 1 ]; then
  usage
  exit
fi

for k in "$@"
  do
    case $k in
      -f=*|--input-file=*)
      INPUT="${k#*=}"
      shift
      ;;
      *)
      usage 
      exit     # unknown option
      ;;
    esac
    shift
  done

COMBI_FILE='/home/ripley/combinations_14'
SPARK_HOME='/opt/spark-1.5.0-bin-hadoop2.6/'
APP_NAME='WordCount'

# Annoying pattern match a line such as: ((13,2),(88,66,44))
regex="^\(\(([0-9]+),([0-9])\)\,\(([0-9]+),([0-9]+),([0-9]+)\)\)"
for conf in `cat ${COMBI_FILE}`
#for run in $(eval echo {0..${RUN_NB}})
  do
#    selected=`shuf -n 1 ${COMBI_FILE}`
#    if [[ $selected =~ $regex ]]; then
    if [[ $conf =~ $regex ]]; then
      echo "Picking up: $conf"
      mem_node=${BASH_REMATCH[1]}
      cores_node=${BASH_REMATCH[2]}
      executors=${BASH_REMATCH[3]}
      mem_total=${BASH_REMATCH[4]}
      cores_total=${BASH_REMATCH[5]}
      total_exec_cores=$((${executors} * ${cores_node}))

      combination=${executors}-${mem_node}-${cores_node}-${APP_NAME}-${INPUT}

      cmd=`${SPARK_HOME}bin/spark-submit --driver-java-options "-javaagent:/opt/btrace/build/btrace-agent.jar=unsafe=true,scriptOutputFile=/tmp/${combination}-Driver,script=/opt/spark-1.5.0-bin-hadoop2.6/DriverProbe.class -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/${combination}-DriverGc -Xmx12048m -Xms12048m" --conf "spark.executor.extraJavaOptions=-javaagent:/opt/btrace/build/btrace-agent.jar=unsafe=true,scriptOutputFile=/tmp/${combination}-Exec,script=/opt/spark-1.5.0-bin-hadoop2.6/ExecutorProbe.class -XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --class WordCount --jars /home/ripley/scala/wc/wc.jar --executor-memory ${mem_node}G --executor-cores ${cores_node} --total-executor-cores ${total_exec_cores} /home/ripley/scala/wc/src/WordCount.scala hdfs://152.3.145.69:60070/${INPUT}`
      if [[ $? == 0 ]]; then
        app_id=`ls -t /opt/spark-1.5.0-bin-hadoop2.6/logs/app* | head -n 1 | cut -d'/' -f 5`
        /home/ripley/result_crawler.sh -c=${combination} -i=${app_id} -m -w
        /home/ripley/back_crawler.sh -c=${combination} -i=${app_id}
      else
        rm /tmp/${combination}*
      fi

      hdfs dfs -rm -r -f /wcresults

      sleep 5
    fi
  done 
