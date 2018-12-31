import sys
import os
from heapq import heappush, heappop
import glob

class LineEntry:
	def __init__(self, embedding):
		self.embedding = embedding 
	def __lt__(self,other):
		return self.embedding[0] > other.embedding[0]
	def __eq__(self,other):
		return self.embedding[0] == other.embedding[0]

h = []

folder = sys.argv[1]
#filename = '0'
#file = open(filename, "r")
print("Folder", folder)

nums_set = set()

for filepath in glob.glob(folder + '/*'):
	file = open(filepath, "r")

	for line in file:
		nums = [int(n) for n in line.split()]
		nums.sort(reverse=True)
		nums_str = ' '.join(str(x) for x in nums)
		if nums_str in nums_set:
			continue
		nums_set.add(nums_str)
		x = LineEntry(nums)
		heappush(h, x)

#new_fname = "sorted-" + os.path.basename(filename)
new_fname = sys.argv[2]
print(new_fname)
with open(new_fname, 'w') as f:
	for i in range(len(h)):
		l = ' '.join(str(x) for x in heappop(h).embedding)
		f.write(l + '\n')
