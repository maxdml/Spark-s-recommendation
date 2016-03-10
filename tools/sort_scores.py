#!/usr/bin/python

from optparse import OptionParser
import json

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename",
                  help="scores files", metavar="FILE")

(options, args) = parser.parse_args()
wch = open(options.filename, 'r')

scores = {}

for s in wch.readlines():
  s = s.rstrip('\r\n')
  s = s.split(',')
  app_id = s[0]
  if not app_id in scores.keys():
    scores[app_id] = [(s[1], float(s[2]))]
  else:
    scores[app_id].append((s[1], float(s[2])))

for s in scores:
  print('app %s:' % s)
  sort = sorted(scores[s], key=lambda x: x[1])
  for t in sort:
    print(t)
