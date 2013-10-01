import MapReduce
import sys

"""
Multiply  in the Simple Python MapReduce Framework
"""


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    matrix = record[0]
    row, col = record[1:3]
    value = record[3]
    for j in range(5):
        if matrix == 'a':
            mr.emit_intermediate((row, j), (col, matrix, value))
        else:
            mr.emit_intermediate((j, col), (row, matrix, value))
    

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    list_of_values.sort()
    total = 0
    for i in range(len(list_of_values)-):
       idxA = list_of_values[i][0]
       idxB = list_of_values[i+1][0]
       if idxA == idxB:
           total += list_of_values[i][2] * list_of_values[i+1][2]

    mr.emit((key[0], key[1], total))


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)



