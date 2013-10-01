import MapReduce
import sys

"""
Assignment 1in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    identifier = record[0]
    sequence = record[1]
    mr.emit_intermediate(sequence[:-10], identifier)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    mr.emit(key)


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
