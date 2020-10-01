from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
class Top_10_movies(MRJob):
	def movie_title(self, movid):
		with open("C:/Users/welcome/Desktop/Python Complete/MRJob/u.item", "r") as file:
			dataset = csv.reader(file, delimiter='|')
			next(dataset)
			for line in dataset:
				if int(movid) == int(line[0]):
					return line[1]
	def steps(self):
		return [
			MRStep(mapper=self.mapper_1, reducer=self.reducer_1),
		MRStep(mapper=self.mapper_2, reducer=self.reducer_2)
		]
	def mapper_1(self, _, line):
		(userID, movieID, rating, timestamp) = line.split('\t')
		yield movieID, rating
	def reducer_1(self, key, values):
		i,totalRating,cnt=0,0,0
		for i in values:
			totalRating += int(i)
			cnt += 1
		if cnt>=100:
			yield key, totalRating/float(cnt)
	def mapper_2(self, key, values):
		yield None, (values, key)
	def reducer_2(self, _, values):
		i=0
		for rating, key in sorted(values, reverse=True):
			i+=1
			if i<=10:
				yield (key,rating), self.movie_title(int(key))
if __name__ == '__main__':
	Top_10_movies.run()