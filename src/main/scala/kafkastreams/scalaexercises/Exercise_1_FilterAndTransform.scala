package kafkastreams.scalaexercises

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.{Integer, String}
import org.apache.kafka.streams.scala.StreamsBuilder

import scala.util.control.NonFatal

class Exercise_1_FilterAndTransform {

  private implicit val json = new JsonNodeSerde

  /**
    * Read the Kafka topic 'text' and send the contents directly to
    * the new topic 'pass-through'
    */
  def passEventsThroughDirectly(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the Kafka topic 'text', convert each line of text to the
    * length of that text and send it to the topic 'line-lengths'
    * as a stream of ints
    */
  def lineLengths(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the Kafka topic 'text', count the number of words in
    * each line and send that to the topic 'words-per-line' as a
    * stream of ints
    */
  def wordsPerLine(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the Kafka topic 'text', find the lines containing the
    * word 'conference' and send them to the topic
    * 'contains-conference'
    */
  def linesContainingConference(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the Kafka topic 'text', split each line into words and
    * send them individually to the topic 'all-the-words'
    */
  def allTheWords(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the Kafka topic 'click-events' as json, get the object URL
    * (see 'ClickEvents' class in the 'testdata' package for details)
    * and send the URL as a string to the topic 'urls-visited'
    */
  def urlsVisited(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the Kafka topic 'click-events' as json, find the events
    * that are for objects of @type 'Article' (see 'ClickEvents'
    * class in the 'testdata' package for details) and send the
    * events unmodified to the topic 'articles' as json
    */
  def articles(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the Kafka topic 'click-events' as json, find the events
    * that are for objects of @type 'Article' and send the object
    * URLs to the topic 'article-urls' as strings
    */
  def articleVisits(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the Kafka topic 'click-events' as json, find the events
    * that are for objects of @type 'ClassifiedAd' and send the
    * object prices to the topic 'classified-ad-prices' as ints
    */
  def classifiedAdPrices(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the Kafka topic 'click-events' as json and convert the
    * classified ad events to a simplified format using the provided
    * ValueMapper 'toSimplifiedAd' below:
    *
    * {
    * "title": "The object name",
    * "price": 123 // the object price
    * }
    *
    * Send the resulting events as json to the topic
    * 'simplified-classified-ads'
    */
  def simplifiedClassifiedAds(builder: StreamsBuilder): Unit = {

  }

  private val mapper = new ObjectMapper

  private val toSimplifiedAd: JsonNode => JsonNode =
    ad => mapper.createObjectNode()
      .put("title", ad.path("object").path("name").asText)
      .put("price", ad.path("object").path("price").asInt)

  /**
    * Read the Kafka topic 'click-events' as json and split it into
    * two new streams, one containing Article events (send to
    * 'articles' topic) and the other containing ClassifiedAd events
    * (send to 'classified-ads' topic).
    *
    * You can use the supplied 'objectType' method to create
    * predicates for different event types.
    *
    * Can you think of more than one way to solve it?
    */
  def splitArticlesAndAds(builder: StreamsBuilder): Unit = {

  }

  def objectType(`type`: String) =
    (key: String, json: JsonNode) => json.path("object").path("@type").asText == `type`

  /**
    * Read the Kafka topic 'click-events' as strings and filter out
    * events that are invalid json. Send the correct events to the
    * topic 'json-events' as json.
    *
    * You can use the supplied 'tryParseJson' method to handle
    * parsing and error handling.
    */
  def filterOutInvalidJson(builder: StreamsBuilder): Unit = {

  }

  def tryParseJson = (event: String) => try {
    List(mapper.readTree(event))
  } catch {
    case NonFatal(ex) => Nil
  }

}
