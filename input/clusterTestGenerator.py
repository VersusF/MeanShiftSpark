from sklearn.datasets.samples_generator import make_blobs
centers = [[52, -6], [55, -3], [57, 0], [52, 0], [57, -6]]
X, _ = make_blobs(n_samples=2000, centers=centers, cluster_std=0.005,
                  n_features=2)

with open('artificial_clusters.csv', 'w') as fout:
    for p in X:
        fout.write('"p","","","","","","{}","{}",""\n'.format(p[0], p[1]))
