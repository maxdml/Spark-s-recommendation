# Spark-s-recommendation
Code base to validate and give recommendations to Apache Spark users

Please contact me to get a copy of the project report.

Things are a bit slowed down by my TA duties. However I am working hard on the theoritial fundation
of collaborative filtering in order to increase the model's accuracy :)

## TODO:
* write up the daemon to wrap all the workflow
* format recommendation output to JSON
* ensemble recommendation (multiple constraints)
* document all naming formats
* study a possible integration with https://issues.apache.org/jira/browse/SPARK-9850 to resize executor size after
a map stage in function to the chosen query plan.

## HOWTO USE
### submit applications:
* submit.py uses a JSON configuration file one can use to setup application parameters
* all logs are then put in a directory (event logs, JVM logs, etc) and 'public' directories such as /tmp are cleaned up

### launch the daemon:

### consult recommendations:

## Validating the model 
RMSE and some blend of AUC are computed in rs_validation. We use Spark ALS to train the model.

## Miscelaneous:
* auto run scripts allow to exhaustively run applications on a set of configuration. Data are automatically gathered and backed up.
* Cartesian.scala is a tool allowing to generate a sequence of configuration based on a node capacity and quantity.
