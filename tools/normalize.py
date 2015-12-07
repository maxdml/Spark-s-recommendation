from optparse import OptionParser
from collections import defaultdict
from decimal import *

def findMax(rankings):
  mx = 0
  for ranking in rankings:
    if ranking[1] > mx:
      mx = ranking[1]

  return mx

def findMin(rankings):
  mn = 999999999 
  for ranking in rankings:
    if ranking[1] < mn:
      mn = ranking[1]

  return mn

def normalize(n, mn, mx):
  return round(1 - float((n - mn) / (mx - mn)), 5)

parser = OptionParser()

parser.add_option("-f", "--file", dest="filename",
                  help="extract score from FILE", metavar="FILE")

(options, args) = parser.parse_args()
score_file = str(options.filename)
normalized_score_file = score_file + '-normalized'

scores = defaultdict(list)

f = open(score_file, 'r')
for line in f.readlines():
  line = line.rstrip('\r\n')
  (user,product,score) = line.split(',')
  scores[int(user)].append((int(product),int(score)))

f.close()

s = open(normalized_score_file, 'a+')

for u in scores:
  rankings = scores[u]
  print('for user ' + str(u))
  # findMax(rankings)
  mx = Decimal(findMax(rankings))
  print('max: ' + str(mx))
  # findMax(rankings)
  mn = Decimal(findMin(rankings))
  print('min: ' + str(mn))
  for ranking in rankings:
    (product, score) = ranking[0], Decimal(ranking[1])
    #Compute normalized score + write it to a file
    normalized_score = normalize(score, mn, mx)
#    print('before: ' + str(score) + ', after: ' + str(normalize(score, mn, mx)))
    write = str(u) + ',' + str(product) + ',' + str(normalized_score) + "\n"
    s.write(write)

s.close()
