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
        mllibSentiment)
    }

    val oAuth: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()
    val rawTweets = TwitterUtils.createStream(ssc, oAuth)

    val DELIMITER = "¦"
    val tweetsClassifiedPath = PropertiesLoader.tweetsClassifiedPath
    val classifiedTweets = rawTweets.map(predictSentiment)

    classifiedTweets.foreachRDD { rdd =>
      if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {

        rdd.foreach {
      println("Tweet:" + v._1 + " MLlib Analysis : " + mllibSentiment + " and " + "StanfordCoreNLP : " + corenlpSentiment);
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
      // Reduce the RDD memory usage of Spark and improving GC behavior.
      .set("spark.streaming.unpersist", "true")

    val ssc = new StreamingContext(conf, Durations.seconds(PropertiesLoader.microBatchTimeInSeconds))
    ssc
  }


  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  def isTweetInEnglish(status: Status): Boolean = {
    status.getLang == "en" && status.getUser.getLang == "en"
  }

}
