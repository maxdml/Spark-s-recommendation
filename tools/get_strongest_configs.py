#!/usr/bin/python

from optparse import OptionParser
import json

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename",
                  help="scores", metavar="FILE")

(options, args) = parser.parse_args()
fh = open(options.filename, 'r')

scores = {}

for s in fh.readlines():
  s = s.rstrip('\r\n')
  s = s.split(',')
  conf_id = s[1]
  if not conf_id in scores.keys():
    scores[conf_id] = [float(s[2])]
  else:
    scores[conf_id].append(float(s[2]))

for s in scores:
  avg_score = sum(scores[s]) / len(scores[s])
  print('average score for config %s is %s' % (s, avg_score))
