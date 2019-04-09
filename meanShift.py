import math
from numpy import exp
from pyspark import SparkContext

sc = SparkContext("local", "first app")
input_file = "dataset_small.csv"
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


def getChunk(point):
    x, y = point[0], point[1]
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
            result.append((new_x, new_y))
    return chunk_id, result


def merger(points):
    # TODO merge points nearer than EPSILON
    return points


def main():
    # Input file
    my_file = sc.textFile(input_file).cache()
    points = my_file.map(lambda line: line.split(','))
    points = points.map(lambda my_line: (my_line[0], float(my_line[1]),
                                         float(my_line[2])))
    coords = points.map(lambda x: (x[1], x[2]))
    # Generate chunks where every points is associated to at most 9 chunks
    # chunks = coords.map(lambda point: (getChunk(point[0], point[1]),) +
    #                                    point)
    chunks = coords.flatMap(generate_chunks).groupByKey()

    for i in range(ITERATION_NUMBER):
        new_points = chunks.flatMap(mapper)

        print '\n\n\n\n'
        print new_points.collect()
        print '\n\n\n'

        new_points = chunks.map(merger)

        print '\n\n\n\n'
        print new_points.collect()
        print '\n\n\n'


if __name__ == "__main__":
    main()
