from mrjob.job import MRJob 
from mrjob.step import MRStep

class MRMoviePopular(MRJob):
	def steps(self):
		return[
		MRStep(mapper=self.mapper_get_ratings,
			reducer=self.reducer_count_ratings),
		MRStep(reducer=self.reducer_find_max)
		]
	def mapper_get_ratings(self,_,line):
		(userId,moviedId,ratings,timestamp)=line.split('\t')
		yield moviedId, 1

	
	def reducer_count_ratings(self,moviedId,counts):
	    yield None, (sum(counts),moviedId)


	def reducer_find_max(self,moviedId,counts):
		yield max(counts)



if __name__=='__main__':
	MRMoviePopular.run()

