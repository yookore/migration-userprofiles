package com.yookos.migration

import akka.actor.{ Actor, Props, ActorSystem, ActorRef }
import akka.pattern.{ ask, pipe }
import akka.event.Logging
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming.{ Milliseconds, Seconds, StreamingContext, Time }
import org.apache.spark.streaming.receiver._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.mapper._
import com.datastax.spark.connector.cql.CassandraConnector

import org.json4s._
import org.json4s.JsonDSL._
//import org.json4s.native.JsonMethods._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime


/**
 * @author ${user.name}
 */
object UserProfiles extends App {
  
  // Configuration for a Spark application.
  // Used to set various Spark parameters as key-value pairs.
  val conf = new SparkConf(false) // skip loading external settings
  
  val mode = Config.mode
  Config.setSparkConf(mode, conf)
  val cache = Config.redisClient(mode)
  //val ssc = new StreamingContext(conf, Seconds(2))
  //val sc = ssc.sparkContext
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val system = SparkEnv.get.actorSystem

  createSchema(conf)

  implicit val formats = DefaultFormats

  // serializes objects from redis into
  // desired types
  import com.redis.serialization._
  import Parse.Implicits._

  //var count = 0
  var count = new java.util.concurrent.atomic.AtomicInteger(0)
  val totalLegacyUsers = 2124155L
  var cachedIndex = if (cache.get("latest_legacy_profiles_index") == None) 0 else cache.get[Int]("latest_legacy_profiles_index").get

  val usersdf = sqlContext.load("jdbc", Map(
    "url" -> Config.dataSourceUrl(mode, Some("mappings")),
    //"dbtable" -> "users")
    "dbtable" -> f"(SELECT id, username, email, givenname, familyname, phonenumber, created, lastmodified FROM users offset $cachedIndex%d) as users")
  )

  //val profilesRDD = sc.cassandraTable[Profile]("yookore", "legacyuserprofiles").cache()
  //val totalProfiles = profilesRDD.cassandraCount()

  val df = usersdf.select(
    usersdf("id"), usersdf("username"),
    usersdf("email"), usersdf("givenname"),
    usersdf("familyname"), usersdf("phonenumber"),
    usersdf("created"), usersdf("lastmodified"))

  reduce(df)

  usersdf.printSchema()
  
  protected def reduce(df: DataFrame) = {
    df.collect().foreach(row => {
      println("===userids=== " + row(0))
      //count.incrementAndGet()
      cachedIndex = cachedIndex + 1
      cache.set("latest_legacy_profiles_index", cachedIndex)
      upsert(row)
    })
  }

  private def upsert(row: Row) = {

      // Detect number of partitions in an RDD
      // rdd.partitions().size()

      // @todo Run only if total count in Cassandra is less than 2124155
      
      val biography = Some(null)
      val created = row(6).toString()
      val lastupdated = row(7).toString()
      val rstatus = Some(null)
      val username = row.getString(1)
      val hometown = Some(null)
      val firstname = Some(row.getString(3))
      val timezone = Some(null)
      val currentcity = Some(null)
      val userid = row(0).toString()
      val lastname = Some(row.getString(4))
      val birthdate = Some(null)
      val homecountry = Some(null)
      val title = Some(null)
      val currentcountry = Some(null)
      val terms = false
      val gender = Some(null)
      val imageurl = Some(null)
      //val phonenumber = Some(row.getString(4))
      //val mobile = Some(null)
      val homeaddress = Some(null)
      val location = Some(null)

      // Dump computation result to cassandra because
      // Spark and Cassandra are good together
      sc.parallelize(Seq(Profile(
        biography, created, lastupdated, rstatus, username, hometown,
        firstname, timezone, currentcity, userid, lastname, birthdate,
        homecountry, title, currentcountry, terms, gender, imageurl,
        homeaddress, location))).saveToCassandra("yookore", "legacyuserprofiles", 
      SomeColumns("biography", "creationdate", "lastupdated",
        "relationshipstatus", "username", "hometown", "firstname",
        "timezone", "currentcity", "userid", "lastname", "birthdate",
        "homecountry", "title", "currentcountry", "terms", "gender",
        "imageurl", "homeaddress", "location")
      )

        println("===Latest cachedIndex=== " + cache.get[Int]("latest_legacy_profiles_index").get)
  }

  def createSchema(conf: SparkConf): Boolean = {
    val keyspace = Config.cassandraConfig(mode, Some("keyspace"))
    val replicationStrategy = Config.cassandraConfig(mode, Some("replStrategy"))
    CassandraConnector(conf).withSessionDo { sess =>
      //sess.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
      sess.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = $replicationStrategy")
      sess.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.legacyuserprofiles (biography text, relationshipstatus text, userid text, username text, hometown text, firstname text, timezone text, currentcity text, lastname text, birthdate text, homecountry text, title text, currentcountry text, terms boolean, gender text, imageurl text, homeaddress text, location text, creationdate text, lastupdated text, PRIMARY KEY (userid, username)) WITH CLUSTERING ORDER BY (username DESC)")
    } wasApplied
  }
}
