f = open('counts.txt') 
g = open('final_counts.txt', 'w')
for line in f:
   line = line.strip().split()
   g.write(' '.join([ line[0] * int(line[1])]))

