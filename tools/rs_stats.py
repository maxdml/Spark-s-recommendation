from optparse import OptionParser
from subprocess import call

parser = OptionParser()
parser.add_option("-r", "--result_source", dest="results",
                  help="list of result directories", metavar="RESULTS")
parser.add_option("-s", "--space", dest="space",
                  help="number of configurations", metavar="SPACE")
#parser.add_option("-c", "--scores", dest="scores",
#                  help="score file, format: [usr_id,prod_id,ranking]", metavar="SPACE")

(options, args) = parser.parse_args()

#####################################################################################################
# Display configuration space size (product space), number of application and possible configuration#
# Display the total number of results, total % sparsity, and % sparsity per application             #
# Call Spark ALS with a score file name                                                             #
# Display ALS ranking                                                                               #
# Display AUC per application                                                                       #
# Display AUC global                                                                                #
# Display RMSE                                                                                      #
#####################################################################################################


#Parse results directories and map app_id to:
# name (app + input file + input size)
# combination id (e-m-c)
# size 
#Also, create a registry of application and map their name to the number of runs

entries = {}
applications = {}

results = open(options.results, 'r')
for result in results.readlines():
  result = result.rstrip('\r\n')
  run = result.split('/')[3].split('-')

  if (run[7] == 'wikipedia'):
    name = run[6] + run[7] + run[8]
  else:
    name = run[6] + run[7]

  entries[run[1]] = {'name': name, 
                     'combination': run[3] + run[4] + run[5],
                     'size': run[7]}

  if (name in applications):
    applications[name] += 1
  else:
    applications[name] = 1

# event_space = #products * # users
event_space = len(applications) * int(options.space)
# total sparsity = 100 - (#total runs / event space * 100)
sparsity = 100 - float((len(entries) * 100/ event_space))
# same thing per application
app_sparsity = {a: (100 - float(applications[a] * 100 / int(options.space))) for a in applications}

#for a in applications:
#  print(a + ":" + str(applications[a]))
#for e in entries:
#  print(e + ":" + str(entries[e]['combination']))
#print(len(set(entries[e]['combination'] for e in entries))) #if entries[e]['name'] == 'cc5m')))

# Call Spark ALS
#SPARK_HOME='/opt/spark-1.5.0-bin-hadoop2.6/'
#call([SPARK_HOME + 'bin/spark-submit', "--class", "RS", "--jars", "/home/ripley/scala/rs/rs.jar",  "/home/ripley/scala/rs/src/main/RS.scala",  options.scores])

print("###########################################")
print("EVENT SPACE: " + str(event_space))
print("--> CONFIGURATIONS: " + options.space)
print("--> APPLICATIONS: " + str(len(applications)))
print("###########################################")
print("TOTAL TESTS: " + str(len(entries)))
print("TOTAL SPARSITY: " + str(sparsity) + '%')
print("DETAILED APPLICATION SPARSITY: " + str(list(a +":"+ str(app_sparsity[a]) for a in app_sparsity)))
print("###########################################")
