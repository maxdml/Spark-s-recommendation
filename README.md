# Spark-s-recommendation
Code base to validate and give recommendations to Apache Spark users

Please contact me to get a copy of the project report.

## TODO (starting 12/23): 
* write up the daemon to wrap all the workflow
* format recommendation output
* ensemble recommendation (multiple constraints)
* document all naming formats

## HOWTO USE
### submit applications:
* submit.py uses a JSON configuration file one can use to setup application parameters
* all logs are then put in a directory (event logs, JVM logs, etc) and 'public' directories such as /tmp are cleaned up

### launch the daemon:

### consult recommendations:

## Validating the model 
RMSE and some blend of AUC are computed in rs_validation. We use Spark ALS to do so.

## Miscelaneous:
* auto run scripts allow to exhaustively run applications on a set of configuration. Data are automatically gathered and backed up.
* Cartesian.scala is a tool allowing to generate a sequence of configuration based on a node capacity and quantity.

