
import re

lines = open('2009-Obama.txt').readlines()


wc = {}
splitter = re.compile('\W+')

def add(word):
	wc.setdefault(word,0)
	wc[word]+=1

for line in lines:
	[ add(split)  for split in splitter.split(line) if split != '']

for word,count in wc.items():
	print word, count