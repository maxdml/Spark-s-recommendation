#!/usr/bin/python

from optparse import OptionParser
import json

parser = OptionParser()
parser.add_option("-f", "--file", dest="pred_fname",
                  help="predictions", metavar="FILE")

parser.add_option("-a", "--app", dest="app", type='int',
                  help="application", metavar="FILE")

(options, args) = parser.parse_args()
fh = open(options.pred_fname, 'r')

app = options.app - 1

#we do receive results in that order
app_ids = [29147,29128,29126,29124,29123,29105,23211,20321,11632,11631,11622,8742,8741,5864,5863,
           5862,5844,5833,2996,2993,2988,2984,2982,2977,2966,2955]

scores = fh.readlines()
ranks = []
app_scores = scores[app].rstrip('\r\n').split(' ')


for s in range(0, len(app_scores)):
  ranks.append((app_ids[s], float(app_scores[s])))

sort = sorted(ranks, key=lambda x: x[1])

for s in reversed(sort):
  print(s)
