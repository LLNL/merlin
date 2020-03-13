import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors
import yaml

with open('my_data.yaml') as f:
    data = yaml.safe_load(f)


c = [1,2,4,8,16,32]
samples = [1,10,100,1000]
fig, ax = plt.subplots()

for con in c:
    t_per_m = []
    for name, times in data.items():
        if f"c{con}_" in name:
            n_tasks = len(times)
            mins = sum(times) / 60.0
            tasks_per_min = n_tasks / mins
            t_per_m.append(tasks_per_min)
    ax.plot(samples, t_per_m, label=f"{con} workers")

plt.xscale("log")
ax.legend()

ax.set(xlabel='n of samples', ylabel='tasks per minute',
               title='Task speed')
ax.grid()

fig.savefig("task_speed.png")
plt.show()


"""
N_points = 100000
n_bins = 20

# Generate a normal distribution, center at x=0 and y=5

#y = .4 * x + np.random.randn(100000) + 5

#fig, axs = plt.subplots(1, 2, sharey=True, tight_layout=True)

# We can set the number of bins with the `bins` kwarg
#axs[0].hist(x, bins=n_bins)
#axs[1].hist(y, bins=n_bins)
plt.hist(x, bins=n_bins)

plt.show()
"""
