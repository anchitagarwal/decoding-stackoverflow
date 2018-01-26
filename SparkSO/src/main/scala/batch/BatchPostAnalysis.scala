package batch

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.util.Sorting
import java.util.Properties

import com.redis.RedisClient
import com.typesafe.config.{Config, ConfigFactory}

object BatchPostAnalysis {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("stackoverflow")
        val sparkSession = SparkSession.builder().getOrCreate()

        // load property file
        val cf: Config = ConfigFactory.load()
        val hdfsURI: String = cf.getString("aws.hdfsURI")
        val redisURI: String = cf.getString("redis.redisURI")
        val redisPort: Int = cf.getInt("redis.redisPort")

        // load RDDs from HDFS
        val userDF: DataFrame = sparkSession.read.parquet(hdfsURI + "users")
        val postDF: DataFrame = sparkSession.read.parquet(hdfsURI + "posts")

        // maintain a hashset in Redis with posts with their accepted postID
        val acceptedPostDF: DataFrame = postDF.filter(row => {
            row.getInt(1) == 1 && !row.getString(8).equals("")
        })
        // store this dataframe in Redis HS
        acceptedPostDF.rdd.foreachPartition(row => {
            // Redis Client connection
            val redis = new RedisClient(redisURI, redisPort)
            row.foreach(r => {
                redis.hmset("acceptedpost", Map(r.getString(0) -> r.getString(8)))
            })
        })

        // dump the raw users and posts data into Postgresql database
//        insertIntoPostgres(userDF: DataFrame, postDF: DataFrame, cf: Config)

        val postRDD = postDF.rdd.sample(false, 0.1, 42L)

        // transformation: `combineByKey` on `postPairRDD` to group together all posts by a user
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
        val userPostsRDD = postRDD
            .map(row => (row.getString(5), row))
            .combineByKey(createCombiner1, mergeValue1, mergeCombiners1)

        // for a given original post, find all the associated users along with their interaction weights
        val questionUsersRDD = postRDD
            .filter(row => row.getInt(1) == 2)
            .mapPartitions(partition => {
                val redis = new RedisClient(redisURI, redisPort)
                var res: Array[(String, Map[String, Int])] = Array.empty[(String, Map[String, Int])]
                partition.foreach(row => {
                    val tmp: Map[String, String] = redis.hmget[String, String]("acceptedpost", row.getString(9)).get
                    if (tmp.contains(row.getString(9)) && tmp(row.getString(9)).equals(row.getString(0))) {
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
            .reduceByKey((map1, map2) => (map1.keySet ++ map2.keySet).map {i=> (i,map1.getOrElse(i,0) + map2.getOrElse(i,0))}.toMap)

        // for a given user, find all users that are in their 1-degree network
        val userLinkRDD = questionUsersRDD
            .flatMap(row => {
                val value: Map[String, Int] = row._2
                var res: List[(String, Map[String, Int])] = Nil
                value.foreach(pair => {
                    val tmp: Map[String, Int] = (value - pair._1).map(x => (x._1, x._2 + pair._2))
                    res = res :+ (pair._1, tmp)
                })
                res
            })
            .reduceByKey((map1, map2) => (map1.keySet ++ map2.keySet).map {i=> (i,map1.getOrElse(i,0) + map2.getOrElse(i,0))}.toMap)
            .flatMap(row => {
                val uid1: String = row._1
                var res: Seq[((String, String), Int)] = Nil
                row._2.foreach(pair => {
                    res = res :+ ((uid1, pair._1), pair._2)
                })
                res
            })

        // for every user, find the posts that their 1-degree friends are interacting with
        val recommendedPostsRDD = userLinkRDD.map(row => (row._1._2, (row._1._1, row._2)))
            .join(userPostsRDD)
            .map(row => {
                val uid2: String = row._1
                val uid1: String = row._2._1._1
                val wt: Int = row._2._1._2
                val posts: Array[String] = row._2._2
                var res: Array[(String, String, Int)] = Array.empty
                for (i <- posts) {
                    res = res :+ (i, uid2, wt)
                }
                (uid1, res)
            })
            .reduceByKey(_ ++ _)
            .map(row => {
                val uid: String = row._1
                val postArray: Array[(String, String, Int)] = row._2
                Sorting.stableSort(postArray, (i: (String, String, Int), j: (String, String, Int)) => i._3 > j._3)
                (uid, postArray)
            })

        recommendedPostsRDD.take(10).foreach(row => {
            print(row._1 + "\t")
            row._2.foreach(x => print(x + ","))
            println()
        })
    }

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
}