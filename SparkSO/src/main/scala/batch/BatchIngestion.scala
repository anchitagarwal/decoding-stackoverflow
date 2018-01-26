package batch

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object BatchIngestion {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("stackoverflow")
        val sparkSession = SparkSession.builder().getOrCreate()

        // read server details from the config file
        val cf: Config = ConfigFactory.load()
        val s3Bucket = cf.getString("aws.s3Bucket")
        val hdfsURI = cf.getString("aws.hdfsURI")

        // read `users.json` from S3 bucket and load data into RDD
        val usersRDD: RDD[Row] = sparkSession.read.json(s3Bucket + "users.json").rdd.map(
            user => {
                Row(
                    user.getString(5), user.getString(3), user.getString(1), user.getString(2), user.getString(7),
                    Integer.parseInt(user.getString(9)), Integer.parseInt(user.getString(10)),
                    Integer.parseInt(user.getString(4)), Integer.parseInt(user.getString(11))
                )
            }
        )

        // read `posts_questions.json` from S3 bucket and load data into RDD
        val postQuestionsRDD: RDD[Row] = sparkSession.read.json(s3Bucket + "posts_questions.json").rdd.map(
            post => {
                // data cleaning
                var acceptedAnswerId = post.get(0)
                if (acceptedAnswerId == null) {
                    acceptedAnswerId = ""
                }
                var favoriteCount = post.get(6)
                if (favoriteCount == null) {
                    favoriteCount = 0
                } else {
                    favoriteCount = Integer.parseInt(post.getString(6))
                }

                Row(
                    post.getString(7), Integer.parseInt(post.getString(14)), post.getString(17), post.getString(2),
                    post.getString(5), post.getString(13), post.getString(16).split("|"),
                    Integer.parseInt(post.getString(15)), acceptedAnswerId, null, favoriteCount,
                    Integer.parseInt(post.getString(18))
                )
            }
        )

        // read `posts_answers.json` from S3 bucket and load data into RDD
        val postAnswersRDD: RDD[Row] = sparkSession.read.json(s3Bucket + "posts_answers.json").rdd.map(
            post => {
                var favoriteCount = post.get(11)
                if (favoriteCount == null) {
                    favoriteCount = 0
                } else {
                    favoriteCount = Integer.parseInt(post.getString(11))
                }
                Row(
                    post.getString(4), Integer.parseInt(post.getString(12)), null, post.getString(0), post.getString(3),
                    post.getString(10), post.getString(14).split("|"), Integer.parseInt(post.getString(13)), null,
                    post.getString(11), favoriteCount, null
                )
            }
        )

        // read `tags.json` from S3 bucket and load data into RDD
        val tagsRDD: RDD[Row] = sparkSession.read.json(s3Bucket + "tags.json").rdd.map(
            tag => {
                Row(tag.getString(2), tag.getString(3), Integer.parseInt(tag.getString(0)))
            }
        )

        // read `comments.json` from S3 bucket and load data into RDD
        val commentsRDD: RDD[Row] = sparkSession.read.json(s3Bucket + "comments.json").rdd.map(
            comment => {
                Row(
                    comment.getString(1), comment.getString(3), comment.getString(0), comment.getString(2),
                    comment.getString(5)
                )
            }
        )

        // combine postQuestionsRDD and postAnswersRDD into a single postsRDD for optimized calculation
        val rawPostsRDD: RDD[Row] = postQuestionsRDD.++(postAnswersRDD)
        // remove posts that doesn't contain owner user ID
        val postsRDD: RDD[Row] = rawPostsRDD.filter(post => post.get(5) != null)

        // store the RDDs into HDFS as Parquet files to retrieve them efficiently later
        // usersRDD
        val userStruct = StructType(
            StructField("id", StringType, false) ::
            StructField("name", StringType, false) ::
            StructField("age", StringType, false) ::
            StructField("creationDate", StringType, false) ::
            StructField("location", StringType, false) ::
            StructField("reputation", IntegerType, false) ::
            StructField("upVotes", IntegerType, false) ::
            StructField("downVotes", IntegerType, false) ::
            StructField("views", IntegerType, false) ::
            Nil
        )
        val usersDf = sparkSession.createDataFrame(usersRDD, userStruct)
        usersDf.write.format("parquet").mode("overwrite").save(hdfsURI + "users/")

        // postsRDD
        val postStruct = StructType(
            StructField("id", StringType, false) ::
            StructField("postTypeId", IntegerType, false) ::
            StructField("title", StringType, true) ::
            StructField("body", StringType, false) ::
            StructField("postDate", StringType, false) ::
            StructField("ownerUserId", StringType, false) ::
            StructField("tags", ArrayType(StringType, false), false) ::
            StructField("score", IntegerType, false) ::
            StructField("acceptedAnswerId", StringType, true) ::
            StructField("parentId", StringType, true) ::
            StructField("favoriteCount", IntegerType, true) ::
            StructField("viewCount", IntegerType, true) ::
            Nil
        )
        val postsDf = sparkSession.createDataFrame(postsRDD, postStruct)
        postsDf.write.format("parquet").mode("overwrite").save(hdfsURI + "posts/")

        // tagsRDD
        val tagStruct = StructType(
            StructField("id", StringType, false) ::
            StructField("tagName", StringType, false) ::
            StructField("count", IntegerType, false) ::
            Nil
        )
        val tagsDf = sparkSession.createDataFrame(tagsRDD, tagStruct)
        tagsDf.write.format("parquet").mode("overwrite").save(hdfsURI + "tags/")

        // commentsRDD
        val commentStruct = StructType(
            StructField("id", StringType, false) ::
            StructField("text", StringType, false) ::
            StructField("creationDate", StringType, false) ::
            StructField("postId", StringType, false) ::
            StructField("userId", StringType, false) ::
            Nil
        )
        val commentDf = sparkSession.createDataFrame(commentsRDD, commentStruct)
        commentDf.write.format("parquet").mode("overwrite").save(hdfsURI + "comments/")
    }
}