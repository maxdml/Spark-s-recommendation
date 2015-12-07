#!/bin/bash

function usage {
  echo 'usage: ./result_crawler.sh -c|--combination <#cores, #memory, #executors> -i|--application-id [-m|--master] [-w|--workers]'
  echo 'example: ./result_crawler.sh -c=5-4-20, -i=app-20151009210856-0001 -n=kmeans'
}

if [ $# -lt 2 ]; then
  usage
  exit
fi

for k in "$@"
  do
    case $k in
      -c=*|--combination=*)
      COMBINATION="${k#*=}"
      shift
      ;;
      -n=*|--name=*)
      APP_NAME="${k#*=}"
      shift
      ;;
      -i=*|--application-id=*)
      APP_ID="${k#*=}"
      shift
      ;;
      -m|--master)
      MASTER=true
      shift
      ;;
      -w|--worker)
      WORKER=true
      shift
      ;;
      *)
      usage 
      exit     # unknown option
      ;;
    esac
    shift
  done

RESULT_DIR='/var/results/'${APP_ID}'-'${COMBINATION}
DRIVER_BTRACE='/tmp/'${COMBINATION}'-Driver'
SPARK_HOME='/opt/spark-1.5.0-bin-hadoop2.6/'
DRIVER_GC='/tmp/'${COMBINATION}'-DriverGc'
SLAVES=$SPARK_HOME'conf/slaves'
EVENT_LOG=${SPARK_HOME}'logs/'${APP_ID}
MASTER_LOG=${SPARK_HOME}'logs/spark-ripley-org.apache.spark.deploy.master.Master-1-mother.out'

echo ${COMBINATION}

#Create result directory
if [ ! -d '/var/results' ]; then
  mkdir '/var/results'
fi

echo 'Creating '${RESULT_DIR}
mkdir ${RESULT_DIR}

#Get driver GC logs
echo 'Getting driver GC logs '${DRIVER_GC}
cp ${DRIVER_GC} ${RESULT_DIR}

#Get driver BTrace logs
echo 'Getting driver BTrace log '${DRIVER_BTRACE}'xxx'
cp ${DRIVER_BTRACE}* ${RESULT_DIR}

#Get event log
echo 'Getting event log '${EVENT_LOG}
cp ${EVENT_LOG} ${RESULT_DIR}

#Get Master log (optional)
if [ $MASTER ]; then
  echo 'Getting master log '${MASTER_LOG}
  cp ${MASTER_LOG} ${RESULT_DIR}
fi

for slave in `cat ${SLAVES}`; do
  NAME=`echo ${slave} | cut -d'@' -f 2`
  SLAVE_DIR=${RESULT_DIR}'/'${NAME}
  echo 'Gathering data from '${SLAVE}
  echo 'Creating slave dir: '${SLAVE_DIR}
  mkdir ${SLAVE_DIR}
  
  #Get Worker logs (optional)
  if [ $WORKER ]; then
    echo 'Getting worker logs: '${SLAVE_DIR}
    scp ${slave}:${SPARK_HOME}'/logs/spark-mother-org.apache.spark.deploy.worker.Worker-1-'${NAME}'.out' ${SLAVE_DIR}
  fi

  #Get Executors JVM logs
  for executor in `ssh $slave 'ls' ${SPARK_HOME}/work/${APP_ID}`; do
    echo 'Getting excutor '$executor' stdout and stderr'
    scp $slave:${SPARK_HOME}'work/'${APP_ID}'/'$executor'/stderr' ${SLAVE_DIR}'/stderr-'$executor
    scp $slave:${SPARK_HOME}'work/'${APP_ID}'/'$executor'/stdout' ${SLAVE_DIR}'/stdout-'$executor
  done

  #Get Executors BTrace logs
  scp $slave:'/tmp/'${COMBINATION}'-Exec'* ${SLAVE_DIR}
done
