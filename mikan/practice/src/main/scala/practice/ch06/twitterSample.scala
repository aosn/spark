package practice.ch06

import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object twitterSample {

  val sc = new SparkContext()
  val ssc = new StreamingContext(sc, Seconds(5))

  System.setProperty("twitter4j.oauth.consumerKey", "xxx")
  System.setProperty("twitter4j.oauth.consumerSecret", "xxx")
  System.setProperty("twitter4j.oauth.accessToken", "xxx-xxx")
  System.setProperty("twitter4j.oauth.accessTokenSecret", "xxx")

  val twitterStream = TwitterUtils.createStream(ssc, None)
  val tweets = twitterStream.map(tweet => tweet.getUser.getId + ":" + tweet.getText)
  tweets.print

  ssc.start
}
