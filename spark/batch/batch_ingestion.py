from pyspark import SparkContext, SparkConf

class batch_ingestion:
	"""
	Class that loads all relevant files from S3 to Spark RDD's
	"""
	def __init__(self):
		"""
		Method to initialize the class
		"""
		self.conf = SparkConf().setAppName("posts_ingestion")
		self.sc = SparkContext(conf=self.conf)

	def ingestion(self):
		"""
		This method loads the StackOverflow CSV files from S3
		"""
		self.users = self.sc.textFile("s3a://insight-so-bucket/users.csv", 
			minPartitions=6)
		self.tags = self.sc.textFile("s3a://insight-so-bucket/tags.csv", 
			minPartitions=6)
		self.comments = self.sc.textFile("s3a://insight-so-bucket/comments.csv", 
			minPartitions=6)
		self.posts_questions = self.sc.textFile("s3a://insight-so-bucket/posts_questions.csv", 
			minPartitions=6)
		self.posts_answers = self.sc.textFile("s3a://insight-so-bucket/posts_answers.csv", 
			minPartitions=6)

	def preprocessing(self):
		"""
		Method to preprocess the raw data and persist it efficiently for future analysis
		"""
		

def main():
	"""
	Main method that handles the input arguments and call the appropriate function.

	Input:
		argv: command line arguments
	"""
	bi = batch_ingestion()
	bi.ingestion()

if __name__ == '__main__':
	main()