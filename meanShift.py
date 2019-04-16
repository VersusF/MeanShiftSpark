import math
from numpy import exp
from pyspark import SparkContext

sc = SparkContext("local", "first app")
sc.setLogLevel("ERROR")
input_file = "dataset.csv"
MAX_X = 100
MAX_Y = 100
DISTANCE_LIMIT = 3
EPSILON = 0.5
CHUNK_SIZE = 10
ITERATION_NUMBER = 1
MAX_CHUNK_X = math.floor(MAX_X / CHUNK_SIZE) - 1
MAX_CHUNK_Y = math.floor(MAX_Y / CHUNK_SIZE) - 1


def distance(x, y):
    return math.sqrt((x[0] - y[0]) ** 2 + (x[1] - y[1]) ** 2)


def gaussian_kernel(distance, bandwidth):
    val = (1/(bandwidth*math.sqrt(2*math.pi))) * \
          exp(-0.5*((distance / bandwidth))**2)
    return val


def getChunk(point, y=None):
    if y is None:
        x, y = point[0], point[1]
    else:
        x = point
    return int(x/CHUNK_SIZE), int(y/CHUNK_SIZE)


def generate_chunks(point):
    result = []
    chunk_x, chunk_y = getChunk(point)
    result.append((chunk_x, chunk_y))
    if chunk_x < MAX_CHUNK_X:
        result.append((chunk_x + 1, chunk_y))
        if chunk_y < MAX_CHUNK_Y:
            result.append((chunk_x + 1, chunk_y + 1))
        if chunk_y > 0:
            result.append((chunk_x + 1, chunk_y - 1))
    if chunk_x > 0:
        result.append((chunk_x - 1, chunk_y))
        if chunk_y < MAX_CHUNK_Y:
            result.append((chunk_x - 1, chunk_y + 1))
        if chunk_y > 0:
            result.append((chunk_x - 1, chunk_y - 1))
    if chunk_y < MAX_CHUNK_Y:
        result.append((chunk_x, chunk_y + 1))
    if chunk_y > 0:
        result.append((chunk_x, chunk_y - 1))
    result = map(lambda x: (x,) + (point,), result)
    return result


######################################
#       MAP - REDUCE FUNCTIONS       #
######################################

def mapper(chunk):
    chunk_id, points = chunk[0], chunk[1]
    result = []
    for p in points:
        if getChunk(p) == chunk_id:
            new_x = 0
            new_y = 0
            total_weight = 0
            # Iterates on neighbours
            for candidate in points:
                d = distance(p, candidate)
                if d <= DISTANCE_LIMIT:
                    # calculate mean
                    weight = gaussian_kernel(d, DISTANCE_LIMIT)
                    new_x += candidate[0]*weight
                    new_y += candidate[1]*weight
                    total_weight += weight
            # do the shift
            new_x /= total_weight
            new_y /= total_weight
            # new_point = (getChunk(new_x, new_y), (new_x, new_y))
            new_chunks = generate_chunks((new_x, new_y))
            result.extend(new_chunks)
    return result


# def merger(points, x):
#     # merge points nearer than EPSILON
#     # print 'merging', x, 'with', points
#     for y in points:
#         if distance(x, y) <= EPSILON:
#             return points
#     points.add(x)
#     return points


def combiner(set_a, set_b):
    # combines two sets merging
    # print 'combining', set_b, 'with', set_a
    for x in set_b:
        should_add = True
        for y in set_a:
            # if two points are near do not add
            if distance(x, y) <= EPSILON:
                should_add = False
                break
        if should_add:
            set_a.add(x)
    return set_a


def unwrap(chunked_centroids):
    chunk_id, centroids = chunked_centroids[0], chunked_centroids[1]
    result = []
    for centroid in centroids:
        if getChunk(centroid) == chunk_id:
            result.append(centroid)
    return result


def grouper(points, new_point):
    points.add(new_point)
    return points


def toString(point):
    return str(point[0]) + ',' + str(point[1])


def main():
    # Input file
    my_file = sc.textFile(input_file).cache()
    points = my_file.map(lambda line: line.split(','))
    points = points.map(lambda my_line: (my_line[0], float(my_line[1]),
                                         float(my_line[2])))
    coords = points.map(lambda x: (x[1], x[2]))

    # Generate chunks where every points is associated to at most 9 chunks
    chunks = coords.flatMap(generate_chunks).foldByKey(set(), grouper)

    # MAP
    new_points = chunks
    for i in range(ITERATION_NUMBER):
        new_points = new_points.flatMap(mapper).foldByKey(set(), grouper)

    # REDUCE
    # centroids = new_points.aggregateByKey(set(), merger, combiner)
    print '\n\n\n\t\t\tREDUCING\n\n\n'
    centroids = new_points.foldByKey(set(), combiner)
    centroids = centroids.flatMap(unwrap)
    centroids.map(toString).saveAsTextFile('output')


if __name__ == "__main__":
    main()
