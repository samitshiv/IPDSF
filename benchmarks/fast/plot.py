import os

import matplotlib.pyplot as plt
import numpy as np

keys = []
speeds = {}

with open("res.csv") as f:
    for line in f:
        l = line.split(",")
        keys.append(int(l[0]))
        p = int(l[1])
        if p not in speeds:
            speeds[p] = []
        speeds[p].append(float(l[2]))

keys = list(dict.fromkeys(keys))
print(keys)
print(speeds)

ind = np.arange(len(speeds[50]))
width = 0.5

fig, ax = plt.subplots()
rects50 = ax.bar(ind - width/2, speeds[50], width / 2, color="tab:blue", label="50 partitions")
rects100 = ax.bar(ind, speeds[100], width / 2, color="tab:orange", label="100 partitions")
rects150 = ax.bar(ind + width/2, speeds[150], width / 2, color="tab:green", label="150 partitions")

ax.set_title("performance by number of consumers")
ax.set_ylabel("MB/s")
ax.set_xticks(ind)
ax.set_xticklabels(keys)
ax.legend()

plt.savefig("res.png")
