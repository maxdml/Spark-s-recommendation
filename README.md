#NOTE: I am currently working on a more advanced profile warehouse for Apache Spark. One key feature is that I will not use the event log file directly to gather data, but rather the monitoring rest API. This repo is kept as legacy.

# Spark-s-recommendation
Code base to validate and give recommendations to Apache Spark users. We propose a system which
gathers data (event logs, btrace logs) every time an application is ran through Spark.

Periodically, the system triggers a recommender engine which parses a database of pre-processed logs, and
propose new configurations for an application. Recommendations are made based on a constraint
users can define in a configuration file.

While I am working on the todo list bellow, the actual focus is on the insight module, which
plots the behaviour of executors and driver for an application. The goal is to hilight
interesting features of apps' execution, which might be helpful for users to configure better
the system in the future, detect bottlenecks, and pave the way for optimizations.

I choosed to make the insight module the same as the DB module, for simplicity. That should be
redefined in the future.

## TODO:
* Create GC log in the insights module 
* Write up the daemon to wrap all the workflow
* Format recommendation output to JSON
* Ensemble recommendation (multiple constraints)
* Document all naming formats
* study a possible integration with https://issues.apache.org/jira/browse/SPARK-9850 to resize executor size after
a stage in function of the chosen query plan.

##REQUIRED LIBRARIES
* Numpy

## HOWTO USE
### submit applications:
* submit.py uses a (rough :) JSON configuration file one can use to setup application parameters
* all logs are then put in a directory (event logs, JVM logs, etc) and 'public' directories such as /tmp are cleaned up

### launch the daemon:

### consult recommendations:

## Validating the model 
RMSE and some blend of AUC are computed in rs_validation. We use Spark ALS and R recommenderlab to validate the model.

## Miscelaneous:
* auto run scripts allow to exhaustively run applications on a set of configuration. Data are automatically gathered and backed up.
* Cartesian.scala is a tool allowing to generate a sequence of configuration based on a node capacity and quantity.
