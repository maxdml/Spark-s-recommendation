from optparse import OptionParser
import json

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename",
                  help="list of event log files", metavar="FILE")

(options, args) = parser.parse_args()
scores_file = 'kmeans4_scores_small'

#Map app to user id
user_dict = {'WordCountwikipedia44g': 1,
             'WordCountwikipedia88g': 8,
             'pagerank1m': 2,
             'cc5m': 3,
             'als': 4,
             'kmeanspoints_5.txt': 5,
             'pagerank5m': 6,
             'kmeanspoints_4.txt': 7,
            }

files = open(options.filename, 'r')

for f in files.readlines():
  f = f.rstrip('\n')
  split = f.split('/')[3].split('-')
  
  if split[7] != 'wikipedia':
    ipt = split[7]
  else:
    ipt = split[7] + split[8]

  user_id = user_dict[split[6] + ipt]

  #Map configuration id to product id
  prod_id = split[3] + split[4] + split[5]

  (start_time, end_time) = 0, 0
  e = open(f, 'r')
  for line in e.readlines():
    j = json.loads(line)
    if j["Event"] == "SparkListenerApplicationStart":
      start_time = j["Timestamp"]
      break
  e.close()

  e = open(f, 'r')
  for line in reversed(e.readlines()):
    j = json.loads(line)
    if j["Event"] == "SparkListenerApplicationEnd":
      end_time = j["Timestamp"]
      break
  e.close()
  
  running_time = end_time - start_time

  #user_id,product_id,rating
  write = str(user_id) + ',' + str(prod_id) + ',' + str(running_time / 1000) + "\n"

  #appending new line to data file
  m = open(scores_file, 'a+')
  m.write(write)
  m.close

files.close
