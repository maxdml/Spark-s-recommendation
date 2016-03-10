#!/usr/bin/python

fname = 'small_14_findable_full'

fh = open(fname, 'r')

configs = {}

for l in fh.readlines():
  l = l.rstrip('\r\n')
  l = l.split('-')

  exe = int(l[0])
  m   = int(l[1])
  c   = int(l[2])

  if (m*exe, c*exe) not in configs:
    configs[(m*exe, c*exe)] = ['-'.join(l)]
  else:
    configs[(m*exe, c*exe)].append('-'.join(l))

#  print('-'.join(l) + ' has ' + str(int(l[0]) * int(l[1])) + ' memory and ' + str(int(l[0]) * int(l[2])) +  ' cores')

for c in configs:
  print(str(configs[c]) + ' have ' + str(c) + ' resources')

