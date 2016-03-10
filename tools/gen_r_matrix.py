#/usr/bin/python

from optparse import OptionParser
import json

parser = OptionParser()
parser.add_option("-f", "--file", dest="filename",
                  help="score file", metavar="FILE")

(options, args) = parser.parse_args()

fh = open(options.filename, 'r')

data = []

for line in fh.readlines():
  line = line.rstrip('\n\r')
  line = line.split(',')

  data.append((line[1], line[2]))

fh.close()

output = 'new_matrix.csv'

data = sorted(data, key = lambda s: int(s[0]), reverse=True)

print(data)

headers = ''
row = ''
for d in data:
  headers += d[0] + ','
  row += d[1] + ','

fh = open(output, 'a')
fh.write(headers + '\n')
fh.write(row + '\n')
fh.close()
