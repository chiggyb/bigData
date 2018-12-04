package org.p7h.spark.sentiment

import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.p7h.spark.sentiment.corenlp.CoreNLPSentimentAnalyzer
import org.p7h.spark.sentiment.mllib.MLlibSentimentAnalyzer
import org.p7h.spark.sentiment.utils._
import redis.clients.jedis.Jedis
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization

/**
  * Analyzes and predicts Twitter Sentiment in [near] real-time using Spark Streaming and Spark MLlib.
  * Uses the Naive Bayes Model created from the Training data and applies it to predict the sentiment of tweets
  * collected in real-time with Spark Streaming, whose batch is set to 20 seconds.
  * Raw tweets [compressed] and also the gist of predicted tweets are saved to the disk.
  */

object TweetSentimentAnalyzer {

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(createSparkStreamingContext)
    val simpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy")

    LogUtils.setLogLevels(ssc.sparkContext)

    val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)
    val stopWordsList = ssc.sparkContext.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))

      def predictSentiment(status: Status): (Long, String, String, Int, Int, Double, Double, String, String) = {
      val tweetText = replaceNewLines(status.getText)
      val (corenlpSentiment, mllibSentiment) = {
        // If tweet is in English, compute the sentiment by MLlib and also with Stanford CoreNLP.
        if (isTweetInEnglish(status)) {
          (CoreNLPSentimentAnalyzer.computeWeightedSentiment(tweetText),
            MLlibSentimentAnalyzer.computeSentiment(tweetText, stopWordsList, naiveBayesModel))
        } else {
          (0, 0)
        }
      }
      (status.getId,
        status.getUser.getScreenName,
        tweetText,
        corenlpSentiment,
        mllibSentiment,
        status.getGeoLocation.getLatitude,
        status.getGeoLocation.getLongitude,
        status.getUser.getOriginalProfileImageURL,
        simpleDateFormat.format(status.getCreatedAt))
    }

    val oAuth: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()
    val rawTweets = TwitterUtils.createStream(ssc, oAuth)

    if (PropertiesLoader.saveRawTweets) {
      rawTweets.cache()

      rawTweets.foreachRDD { rdd =>
        if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
          saveRawTweetsInJSONFormat(rdd, PropertiesLoader.tweetsRawPath)
        }
      }
    }
    val DELIMITER = "¦"
    val tweetsClassifiedPath = PropertiesLoader.tweetsClassifiedPath
    val classifiedTweets = rawTweets.filter(hasGeoLocation)
      .map(predictSentiment)

    classifiedTweets.foreachRDD { rdd =>
      if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
        saveClassifiedTweets(rdd, tweetsClassifiedPath)

        rdd.foreach {
          case (id, screenName, text, sent1, sent2, lat, long, profileURL, date) => {
            val sentimentTuple = (id, screenName, text, sent1, sent2, lat, long, profileURL, date)
            // TODO -- Need to remove this and use "Spark-Redis" package for publishing to Redis.
            // TODO -- But could not figure out a way to Publish with "Spark-Redis" package though.
            // TODO -- Confirmed with the developer of "Spark-Redis" package that they have deliberately omitted the method for publishing to Redis from Spark.
            val jedis = new Jedis("localhost", 6379)
            val pipeline = jedis.pipelined
            val write = sentimentTuple.productIterator.mkString(DELIMITER)
            val p1 = pipeline.publish("TweetChannel", write)
            //println(p1.get().longValue())
            pipeline.sync()
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(PropertiesLoader.totalRunTimeInMinutes * 60 * 1000) // auto-kill after processing rawTweets for n mins.
  }

  def createSparkStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      // Use KryoSerializer for serializing objects as JavaSerializer is too slow.
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      // For reconstructing the Web UI after the application has finished.
      .set("spark.eventLog.enabled", "true")
      // Reduce the RDD memory usage of Spark and improving GC behavior.
      .set("spark.streaming.unpersist", "true")

    val ssc = new StreamingContext(conf, Durations.seconds(PropertiesLoader.microBatchTimeInSeconds))
    ssc
  }

  def saveClassifiedTweets(rdd: RDD[(Long, String, String, Int, Int, Double, Double, String, String)], tweetsClassifiedPath: String) = {
    val now = "%tY%<tm%<td%<tH%<tM%<tS" format new Date
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    import sqlContext.implicits._
    val classifiedTweetsDF = rdd.toDF("ID", "ScreenName", "Text", "CoreNLP", "MLlib", "Latitude", "Longitude", "ProfileURL", "Date")
    classifiedTweetsDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .save(tweetsClassifiedPath + now)
  }

  def saveRawTweetsInJSONFormat(rdd: RDD[Status], tweetsRawPath: String): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    val tweet = rdd.map(status => jacksonObjectMapper.writeValueAsString(status))
    val rawTweetsDF = sqlContext.read.json(tweet)
    rawTweetsDF.coalesce(1).write
      .format("org.apache.spark.sql.json")
      // Compression codec to compress when saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save(tweetsRawPath)
  }

  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  /**
    * Checks if the tweet Status is in English language.
    * Actually uses profile's language as well as the Twitter ML predicted language to be sure that this tweet is
    * indeed English.
    *
    * @param status twitter4j Status object
    * @return Boolean status of tweet in English or not.
    */
  def isTweetInEnglish(status: Status): Boolean = {
    status.getLang == "en" && status.getUser.getLang == "en"
  }

  def hasGeoLocation(status: Status): Boolean = {
    null != status.getGeoLocation
  }
}