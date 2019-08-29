import math
from numpy import exp
from decimal import Decimal
from pyspark import SparkContext

sc = SparkContext('local', 'first app')
sc.setLogLevel('ERROR')
hadoop_path = 'hdfs://hadoopmaster/user/st-contro/meanShift/'
input_file = 'input/open_pubs_1000.csv'
MULTIPLIER = 100
# TODO set them
MAX_X = Decimal(59.17)
MIN_X = Decimal(49.89)
MAX_Y = Decimal(1.76)
MIN_Y = Decimal(-8.5)
WIDTH_X = MAX_X - MIN_X
WIDTH_Y = MAX_Y - MIN_Y


DISTANCE_LIMIT = Decimal(0.5)
EPSILON = Decimal(0.1)
ITERATION_NUMBER = 1
MAX_CHUNK_X = 100
MIN_CHUNK_X = 0
MAX_CHUNK_Y = 100
MIN_CHUNK_Y = 0
CHUNK_SIZE = 2


def distance(x, y):
    """
    Eucledian distance
    """
    return ((x[0] - y[0]) ** 2 + (x[1] - y[1]) ** 2).sqrt()


def gaussian_kernel(distance, bandwidth):
    val = (1 / (bandwidth * (2 * Decimal(math.pi)).sqrt())) * \
          ((distance / bandwidth) ** 2 / -2).exp()
    return val


def parse_line(line):
    """
    Parse a line returning a tuple with: name, x, y
    longitude and latitude are mapped on a [0,100] interval
    """
    fields = line[1:-1].split('","')  # Get the name, x, y
    # Filter and convert to a tuple
    if len(fields) > 7 and fields[6] != '\\N' and fields[7] != '\\N':
        return fields[0], \
               (Decimal(fields[6]) - MIN_X) / WIDTH_X * 100, \
               (Decimal(fields[7]) - MIN_Y) / WIDTH_Y * 100


def getChunk(point, y=None):
    if y is None:
        x, y = point[0], point[1]
    else:
        x = point
    return int(x/CHUNK_SIZE), int(y/CHUNK_SIZE)


def generate_chunks(point):
    """
    returns a list containing every chunk that can see the point
    """
    result = []
    x, y = point
    chunk_x, chunk_y = getChunk(point)
    # ottengo dimensioni del quadrato interno oltre la distance limit
    lower_x = chunk_x * CHUNK_SIZE + DISTANCE_LIMIT
    upper_x = (chunk_x + 1) * CHUNK_SIZE - DISTANCE_LIMIT
    lower_y = chunk_y * CHUNK_SIZE + DISTANCE_LIMIT
    upper_y = (chunk_y + 1) * CHUNK_SIZE - DISTANCE_LIMIT
    # aggiungo il suo chunk
    result.append((chunk_x, chunk_y))
    # guardo i 3 a destra
    if chunk_x < MAX_CHUNK_X and x >= upper_x:
        result.append((chunk_x + 1, chunk_y))
        if chunk_y < MAX_CHUNK_Y and y >= upper_y:
            result.append((chunk_x + 1, chunk_y + 1))
        if chunk_y > MIN_CHUNK_Y and y <= lower_y:
            result.append((chunk_x + 1, chunk_y - 1))
    # guardo i 3 a sinistra
    if chunk_x > MIN_CHUNK_X and x <= lower_x:
        result.append((chunk_x - 1, chunk_y))
        if chunk_y < MAX_CHUNK_Y and y >= upper_y:
            result.append((chunk_x - 1, chunk_y + 1))
        if chunk_y > MIN_CHUNK_Y and y <= lower_y:
            result.append((chunk_x - 1, chunk_y - 1))
    # guardo sopra e sotto
    if chunk_y < MAX_CHUNK_Y and y >= upper_y:
        result.append((chunk_x, chunk_y + 1))
    if chunk_y > MIN_CHUNK_Y and y <= lower_y:
        result.append((chunk_x, chunk_y - 1))
    # for every chunk generate the tuple chunk, point
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


######################################
#          PRINT FUNCTIONS           #
######################################


def toString(point):
    """
    Remap x and y to longitude and latitude, printing in a good format
    """
    return str(round(point[0] / 100 * WIDTH_X + MIN_X, 6)) + ',' + \
        str(round(point[1] / 100 * WIDTH_Y + MIN_Y, 6))


def main():
    # Input file
    my_file = sc.textFile(hadoop_path + input_file).cache()
    points = my_file.map(parse_line)
    coords = points.filter(lambda x: x is not None).map(lambda x: (x[1], x[2]))
    # print the points for visualization scope
    coords.map(toString).saveAsTextFile(hadoop_path + 'output/points')

    # Generate chunks where every points is associated to at most 9 chunks
    chunks = coords.flatMap(generate_chunks).groupByKey()

    # MAP
    new_points = chunks
    for i in range(ITERATION_NUMBER):
        # map and reduce
        new_points = new_points.flatMap(mapper).groupByKey()

    # REDUCE
    # centroids = new_points.aggregateByKey(set(), merger, combiner)
    centroids = new_points.foldByKey(set(), combiner)
    # save output
    centroids = centroids.flatMap(unwrap)
    centroids.map(toString).saveAsTextFile(hadoop_path + 'output/clusters')
    input()

if __name__ == "__main__":
    main()
