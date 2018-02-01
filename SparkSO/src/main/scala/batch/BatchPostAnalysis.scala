package batch

import org.apache.spark.sql._

import scala.util.Sorting
import java.util.Properties

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object BatchPostAnalysis {
    /**
      * Method to create Map for all accepted posts with question ID as key and accepted answer ID as value
      * @param postDF Post DF
      */
    def getAcceptedPostsMap(postDF: DataFrame): Map[String, String] = {
        /** find all the posts with accepted answer and store them in a HashMap for efficient querying */
        val acceptedPostDF: DataFrame = postDF.filter(row => {
            row.getInt(1) == 1 && !row.getString(8).equals("")
        }).select("id", "acceptedAnswerId")

        var acceptedPostMap: Map[String, String] = Map.empty
        acceptedPostDF.rdd.foreach(row => {
            acceptedPostMap = acceptedPostMap + (row.getString(0) -> row.getString(1))
        })

        acceptedPostMap
    }

    /**
      * Method to store user and post information in Postgres DB
      * @param userDF User DF
      * @param postDF Post DF
      * @param cf Properties file config
      */
    def insertIntoPostgres(userDF: DataFrame, postDF: DataFrame, cf: Config): Unit = {
        // get JDBC info from config file
        val jdbcHostname: String = cf.getString("postgres.jdbcHostname")
        val jdbcPort: Int = cf.getInt("postgres.jdbcPort")
        val jdbcDatabase: String = cf.getString("postgres.jdbcDatabase")
        val jdbcUser: String = cf.getString("postgres.jdbcUser")
        val jdbcPassword: String = cf.getString("postgres.jdbcPassword")

        // dump the user and post information in the PostgreSQL database
        // details for connecting to Postgresql database
        val connectionProperties = new Properties()
        connectionProperties.put("user", jdbcUser)
        connectionProperties.put("password", jdbcPassword)
        connectionProperties.put("driver", "org.postgresql.Driver")

        val jdbcUrl = "jdbc:postgresql://" + jdbcHostname + ":" + jdbcPort + "/" + jdbcDatabase

        // write dataframes to postgres
        userDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "users", connectionProperties)
        postDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "posts", connectionProperties)
    }

    /**
      * Save the recommended posts in MongoDB as key-value pairs
      * @param cf Typesafe Config
      * @param recommendedPosts PairRDD with user as key and recommended posts as values
      */
    def saveRecommendations(cf: Config, recommendedPosts: DataFrame): Unit = {
        val mongoURI: String = cf.getString("mongo.uri")
        val mongoUser: String = cf.getString("mongo.user")
        val mongoPassword: String = cf.getString("mongo.password")

        val writeConfig: WriteConfig =
            WriteConfig(Map("uri" -> s"mongodb://$mongoUser:$mongoPassword@$mongoURI/insight.recommendedPost"))
//        MongoSpark.save(recommendedPosts, writeConfigAccepted)
        MongoSpark.save(recommendedPosts.write, writeConfig)
    }

    /**
      * Main execution method
      * @param args input arguments
      */
    def main(args: Array[String]): Unit = {
        /** get the SparkSession object */
        val sparkSession = SparkSession.builder()
            .appName("decodingStackoverflow")
            .getOrCreate()
        sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

        /** read config details */
        val cf: Config = ConfigFactory.load()
        val hdfsURI: String = cf.getString("aws.hdfsURI")

        /** load user and post tables from HDFS into RDDs */
        val userDF: DataFrame = sparkSession.read.parquet(hdfsURI + "users")
        val postDF: DataFrame = sparkSession.read.parquet(hdfsURI + "posts")

        /** get accepted posts and their accepted answer ID in a map */
        val acceptedPostMap: Map[String, String] = getAcceptedPostsMap(postDF)

        /** store the User and Posts table in Postgres to be later used for UI */
//        insertIntoPostgres(userDF: DataFrame, postDF: DataFrame, cf: Config)

        val postRDD = postDF.rdd.sample(false, 0.1, 42L)
//        val postRDD = postDF.rdd

        /**
          * transformation
          *
          * inputs:
          *     postRDD: from HDFS Parquet files
          * output:
          *     userPostRDD: RDD[(String, Array[String])], list of questions every user has interacted with
          */
        val createCombiner1 = (post: Row) => {
            var res: Array[String] = Array.empty[String]
            if (post.getInt(1) == 1) {
                res = res :+ post.getString(0)
            } else if (post.getInt(1) == 2) {
                res = res :+ post.getString(9)
            }
            res
        }
        val mergeValue1 = (collector: Array[String], post: Row) => {
            var res: Array[String] = collector
            if (post.getInt(1) == 1) {
                res = res :+ post.getString(0)
            } else if (post.getInt(1) == 2) {
                res = res :+ post.getString(9)
            }
            res
        }
        val mergeCombiners1 = (collector1: Array[String], collector2: Array[String]) => {
            val res = collector1 ++ collector2
            res
        }
        val userPostsRDD: RDD[(String, Array[String])] = postRDD
            .map(row => (row.getString(5), row))
            .combineByKey(createCombiner1, mergeValue1, mergeCombiners1)

        /**
          * transformation
          *
          * inputs:
          *     postRDD: from the parquet files
          *
          * output:
          *     questionUsersRDD: RDD[(String, Map[String, Int])], for every post(question), find all associated
          *         weighted users
          */
        // for a given original post, find all the associated users along with their interaction weights
        val questionUsersRDD: RDD[(String, Map[String, Int])] = postRDD
            .filter(row => row.getInt(1) == 2)
            .mapPartitions(partition => {
                var res: Array[(String, Map[String, Int])] = Array.empty[(String, Map[String, Int])]
                partition.foreach(row => {
                    if (acceptedPostMap.contains(row.getString(9)) && acceptedPostMap(row.getString(9)).equals(row.getString(0))) {
                        res = res :+ (row.getString(9), Map(row.getString(5) -> 3))
                    } else {
                        res = res :+ (row.getString(9), Map(row.getString(5) -> 2))
                    }
                })
                res.iterator
            })
            .union(postRDD
                .filter(row => row.getInt(1) == 1)
                .map(row => {
                    (row.getString(0), Map(row.getString(5) -> 4))
                }))
            .reduceByKey((map1, map2) => (map1.keySet ++ map2.keySet)
                .map {i=> (i,map1.getOrElse(i,0) + map2.getOrElse(i,0))}.toMap)

        /**
          * transformation
          *
          * inputs:
          *     questionUsersRDD: RDD[(String, Map[String, Int])], for every post(question), find all associated
          *         weighted users
          * output:
          *     userLinkRDD: RDD[(String, String), Int], every pair of users with their associated weights
          */
        val userLinkRDD: RDD[((String, String), Int)] = questionUsersRDD
            .flatMap(row => {
                val value: Map[String, Int] = row._2
                var res: List[(String, Map[String, Int])] = Nil
                value.foreach(pair => {
                    val tmp: Map[String, Int] = (value - pair._1).map(x => (x._1, x._2 + pair._2))
                    res = res :+ (pair._1, tmp)
                })
                res
            })
            .reduceByKey((map1, map2) => (map1.keySet ++ map2.keySet)
                .map {i=> (i,map1.getOrElse(i,0) + map2.getOrElse(i,0))}.toMap)
            .flatMap(row => {
                val uid1: String = row._1
                var res: Seq[((String, String), Int)] = Nil
                row._2.foreach(pair => {
                    res = res :+ ((uid1, pair._1), pair._2)
                })
                res
            })

        /** Partition userLinkRDD by 2nd user in tuple */
        val numPartitions = 1000
        val userPostCustomPartRDD: RDD[(String, Array[String])] =
            userPostsRDD.partitionBy(new HashPartitioner(numPartitions))
        val userLinkCustomPartRDD: RDD[((String, String), Int)] =
            userLinkRDD.partitionBy(new CustomPartitioner(numPartitions))

        val recommendedPostsRDD: RDD[(String, Array[(String, String, Int)])] =
            userLinkCustomPartRDD.zipPartitions(userPostCustomPartRDD) (
                (iter2, iter1) => {
                    val m = iter1.toMap
                    for {
                        ((u1: String, u2: String), wt: Int) <- iter2
                        if m.contains(u2)
                    } yield (u1, (m(u2), u2, wt))
                }
            )
            .map(row => {
                val u1: String = row._1
                val posts: Array[String] = row._2._1
                val u2: String = row._2._2
                val wt: Int = row._2._3

                var res: Array[(String, String, Int)] = Array.empty
                for (i <- posts) {
                    if (!acceptedPostMap.contains(i)) {
                        res = res :+ (i, u2, wt)
                    }
                }
                (u1, res)
            })
            .reduceByKey(_ ++ _)
            .map(row => {
                val uid: String = row._1
                val postArray: Array[(String, String, Int)] = row._2
                Sorting.stableSort(postArray, (i: (String, String, Int), j: (String, String, Int)) => i._3 > j._3)
                (uid, postArray)
            })

        /** convert recommendationPostsRDD to DF and store in mongoDB */
        val sqlContext = sparkSession.sqlContext
        import sqlContext.implicits._

        val recommendedPostDF: DataFrame = recommendedPostsRDD.toDF("_id", "posts")
        saveRecommendations(cf, recommendedPostDF)
    }
}