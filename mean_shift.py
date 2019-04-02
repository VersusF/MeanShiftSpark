import sys
import csv
from functools import reduce
import math
from numpy import exp

epsilon = 2
distance_limit = 30
iteration_number = 5


points = {}


def distance(x, y):
    return math.sqrt((x[0] - y[0]) ** 2 + (x[1] - y[1]) ** 2)


def gaussian_kernel(distance, bandwidth):
    val = (1/(bandwidth*math.sqrt(2*math.pi))) * \
          exp(-0.5*((distance / bandwidth))**2)
    return val


def mapper(x):
    new_x = 0
    new_y = 0
    total_weight = 0
    for candidate in points.values():
        d = distance(x, candidate)
        if d <= distance_limit:
            weight = gaussian_kernel(d, distance_limit)
            new_x += candidate[0]*weight
            new_y += candidate[1]*weight
            total_weight += weight
    new_x /= total_weight
    new_y /= total_weight
    return new_x, new_y


def reducer(coords, x):
    coords.add((int(x[0]), int(x[1])))
    return coords


def main():
    filename = sys.argv[1]
    with open(filename, 'r') as input_file:
        reader = csv.DictReader(input_file)
        for line in reader:
            points[line['name']] = (int(line['x']), int(line['y']))
    # launch algorithm
    coords = set(points.values())
    # mapper calculates for each point the mean of his neighbours
    # the reducer takes all of this points and if two of them are
    # nearer than the epsilon they are merged. Eventually we have
    # the set of the centroids of the clusters
    for i in range(iteration_number):
        coords = set(map(mapper, coords))
        coords = reduce(reducer, coords, set())
    clusters = {}
    for i, x in enumerate(coords):
        clusters[i] = x
    print(clusters)


if __name__ == "__main__":
    main()
