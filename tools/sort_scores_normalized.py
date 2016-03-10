#!/usr/bin/python

wc_fname = 'wc44_score_small-normalized'

wch = open(wc_fname, 'r')

wc_scores = []

for s in wch.readlines():
  s = s.rstrip('\r\n')
  s = s.split(',')
  wc_scores.append((s[1], float(s[2])))

sort = reversed(sorted(wc_scores, key=lambda x: x[1]))

for s in sort:
  print(s)
