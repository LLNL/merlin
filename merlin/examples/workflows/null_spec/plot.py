import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colors

N_points = 100000
n_bins = 20

# Generate a normal distribution, center at x=0 and y=5
x = np.random.randn(N_points)
#y = .4 * x + np.random.randn(100000) + 5

#fig, axs = plt.subplots(1, 2, sharey=True, tight_layout=True)

# We can set the number of bins with the `bins` kwarg
#axs[0].hist(x, bins=n_bins)
#axs[1].hist(y, bins=n_bins)
plt.hist(x, bins=n_bins)

plt.show()

