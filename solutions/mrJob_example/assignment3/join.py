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
    table = record[0]
    order_id = record[1]
    data = record[2:]
    mr.emit_intermediate(order_id, (table, data))


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    order_table, data_t = list_of_values[0]
    order = [order_table, key] + data_t

    for table, data in list_of_values[1:]:
        order = order + [table, key] + data
        mr.emit(order)
        order = [order_table, key] + data_t
        
    
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
