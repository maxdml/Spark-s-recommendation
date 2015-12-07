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
      shift # past argument
      ;;
      -n=*|--name=*)
      APP_NAME="${k#*=}"
      shift # past argument
      ;;
      -i=*|--application-id=*)
      APP_ID="${k#*=}"
      shift # past argument
      ;;
      -m|--master)
      MASTER=true
      shift # past argument
      ;;
      -w|--worker)
      WORKER=true
      shift # past argument
      ;;
      *)
      usage 
      exit     # unknown option
      ;;
    esac
    shift # past argument or value
  done

RESULT_DIR='/var/results_back/'${APP_ID}'-'${COMBINATION}
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
#mkdir ${RESULT_DIR}

#Get driver GC logs
echo 'Backing up driver GC logs '${DRIVER_GC}
#mv ${DRIVER_GC} ${RESULT_DIR}
rm ${DRIVER_GC}

#Get driver BTrace logs
echo 'Backing up driver BTrace log '${DRIVER_BTRACE}'xxx'
#mv ${DRIVER_BTRACE}* ${RESULT_DIR}
rm ${DRIVER_BTRACE}*

#Get event log
echo 'Backing up event log '${EVENT_LOG}
#cp ${EVENT_LOG} ${RESULT_DIR}

#Get Master log (optional)
if [ $MASTER ]; then
#  echo 'Backing up master log '${MASTER_LOG}
#  cp ${MASTER_LOG} ${RESULT_DIR}
fi

for slave in `cat ${SLAVES}`; do
  NAME=`echo ${slave} | cut -d'@' -f 2`
  SLAVE_DIR=${RESULT_DIR}'/'${NAME}
  
  ssh $slave mkdir -p ${SLAVE_DIR}

  #Get Executors JVM logs
#  for executor in `ssh $slave 'ls' ${SPARK_HOME}/work/${APP_ID}`; do
#    echo 'Backing up excutor '$executor' stdout and stderr'
#    ssh $slave mv ${SPARK_HOME}'work/'${APP_ID}'/'$executor'/stderr' ${SLAVE_DIR}'/stderr-'$executor
#    ssh $slave mv ${SPARK_HOME}'work/'${APP_ID}'/'$executor'/stdout' ${SLAVE_DIR}'/stdout-'$executor
#  done

  #Get Executors BTrace logs
  ssh $slave mv '/tmp/'${COMBINATION}'*-Exec'* ${SLAVE_DIR}
done
