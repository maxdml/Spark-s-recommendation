from optparse import OptionParser

parser = OptionParser()
parser.add_option("-a", "--score1", dest="score1",
                  help="rank file 1", metavar="RESULTS")
parser.add_option("-b", "--score2", dest="score2",
                  help="rank file 2", metavar="RESULTS")

(options, args) = parser.parse_args()

# Just compare the ranking given two rank file of the form
# user_id,product_id,ranking
# /!\ We assume that the number of rows is the same (ie. full measurements or predicted ranking)

def sort1rstAnd3rd(l):
  return l[0], l[2]

# Extract data from source1
s1 = open(options.score1, 'r')
one = []

for line in s1.readlines():
  line  = line.rstrip('\r\n')
  split = line.split(',')
  one.append([split[0], split[1], split[2]])


one.sort(key=sort1rstAnd3rd)

print('user_id, product_id, ranking')
for i in range(0, len(one)):
  print one[i][0] + ', ' + one[i][1] + ', ' + one[i][2] + ' || ' + one[i][0] + ', ' + one[i][1] + ', ' + one[i][2]
