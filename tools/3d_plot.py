from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm
from matplotlib.ticker import LinearLocator, FormatStrFormatter
import matplotlib.pyplot as plt
import numpy as np


# get scores
scores_map = {}

fname = '/home/max/codeZ/rs/daemon/db/bin/wc44_score_small'
fh = open(fname, 'r')

for line in fh.readlines():
  line = line.rstrip('\r\n')
  line = line.split(',')
  combination = line[0]
  rt = int(line[1])

  #get resources
  split = combination.split('-')
  executors = int(split[0])
  mem = int(split[1])
  cores = int(split[2])

  tup = (executors * mem, executors * cores)

  if tup not in scores_map:
    scores_map[tup] = [(combination, rt)]
  else:
    scores_map[tup].append((combination, rt))

x = []
y = []
z = []

for score in scores_map:
  y.append(score[0])
  x.append(score[1])
  z.append(map (min, zip(*scores_map[score]))[1])

fig = plt.figure()
ax = fig.gca(projection='3d')
x, y = np.meshgrid(x, y)
surf = ax.plot_surface(x, y, z, rstride=1, cstride=1, cmap=cm.coolwarm,
                       linewidth=0, antialiased=False)
#ax.set_zlim(-1.01, 1.01)

ax.zaxis.set_major_locator(LinearLocator(10))
ax.zaxis.set_major_formatter(FormatStrFormatter('%.02f'))

ax.invert_xaxis()

fig.colorbar(surf, shrink=0.5, aspect=5)

plt.show()
