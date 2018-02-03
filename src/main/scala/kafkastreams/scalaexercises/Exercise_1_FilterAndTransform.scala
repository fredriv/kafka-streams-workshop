package kafkastreams.scalaexercises

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafkastreams.scalautils.IntSerde
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.scalautils.KafkaStreamsDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{Consumed, StreamsBuilder}

import scala.util.control.NonFatal

class Exercise_1_FilterAndTransform {

  private implicit val strings = Serdes.String
  private implicit val ints = Serdes.Integer
  private implicit val scalaInts = new IntSerde
  private implicit val json = new JsonNodeSerde

  /**
    * Read the Kafka topic 'text' and send the contents directly to
    * the new topic 'pass-through'
    */
  def passEventsThroughDirectly(builder: StreamsBuilder): Unit = {
    val stream = builder.stream("text", Consumed.`with`(strings, strings))
    stream.toS("pass-through")
  }

  /**
    * Read the Kafka topic 'text', convert each line of text to the
    * length of that text and send it to the topic 'line-lengths'
    * as a stream of ints
    */
  def lineLengths(builder: StreamsBuilder): Unit = {
    builder.stream("text", Consumed.`with`(strings, strings))
      .mapValuesS(line => line.length)
      .toS("line-lengths")
  }

  /**
    * Read the Kafka topic 'text', count the number of words in
    * each line and send that to the topic 'words-per-line' as a
    * stream of ints
    */
  def wordsPerLine(builder: StreamsBuilder): Unit = {
    builder.stream("text", Consumed.`with`(strings, strings))
      .mapValuesS(line => line.split(" ").length)
      .toS("words-per-line")
  }

  /**
    * Read the Kafka topic 'text', find the lines containing the
    * word 'conference' and send them to the topic
    * 'contains-conference'
    */
  def linesContainingConference(builder: StreamsBuilder): Unit = {
    builder.stream("text", Consumed.`with`(strings, strings))
      .filter((key, line) => line.contains("conference"))
      .toS("contains-conference")
  }

  /**
    * Read the Kafka topic 'text', split each line into words and
    * send them individually to the topic 'all-the-words'
    */
  def allTheWords(builder: StreamsBuilder): Unit = {
    builder.stream("text", Consumed.`with`(strings, strings))
      .flatMapValuesS(line => line.split(" "))
      .toS("all-the-words")
  }

  /**
    * Read the Kafka topic 'click-events' as json, get the object URL
    * (see 'ClickEvents' class in the 'testdata' package for details)
    * and send the URL as a string to the topic 'urls-visited'
    */
  def urlsVisited(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .mapValuesS(event => event("object")("url").asText)
      .toS("urls-visited")
  }

  /**
    * Read the Kafka topic 'click-events' as json, find the events
    * that are for objects of @type 'Article' (see 'ClickEvents'
    * class in the 'testdata' package for details) and send the
    * events unmodified to the topic 'articles' as json
    */
  def articles(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .filter((key, event) => event("object")("@type").asText == "Article")
      .toS("articles")
  }

  /**
    * Read the Kafka topic 'click-events' as json, find the events
    * that are for objects of @type 'Article' and send the object
    * URLs to the topic 'article-urls' as strings
    */
  def articleVisits(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .filter((key, event) => event("object")("@type").asText == "Article")
      .mapValuesS(event => event("object")("url").asText)
      .toS("article-urls")
  }

  /**
    * Read the Kafka topic 'click-events' as json, find the events
    * that are for objects of @type 'ClassifiedAd' and send the
    * object prices to the topic 'classified-ad-prices' as ints
    */
  def classifiedAdPrices(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .filter((key, event) => event("object")("@type").asText == "ClassifiedAd")
      .mapValuesS(event => event("object")("price").asInt)
      .toS("classified-ad-prices")
  }

  /**
    * Read the Kafka topic 'click-events' as json and convert the
    * classified ad events to a simplified format using JSTL:
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
    val mapper = new ObjectMapper

    val simplifiedClassifiedAd: JsonNode => JsonNode =
      ad => mapper.createObjectNode
        .put("title", ad("object")("name").asText())
        .put("price", ad("object")("price").asInt())

    builder.stream("click-events", Consumed.`with`(strings, json))
      .filter((key, event) => event("object")("@type").asText == "ClassifiedAd")
      .mapValuesS(simplifiedClassifiedAd)
      .toS("simplified-classified-ads")
  }

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
    val clicks = builder.stream("click-events", Consumed.`with`(strings, json))

    val Seq(articles, classifiedAds) = clicks.branchS(objectType("Article"), objectType("ClassifiedAd"))

    articles.toS("articles")
    classifiedAds.toS("classified-ads")

    /*
    clicks.filter(objectType("Article")).toS("articles");
    clicks.filter(objectType("ClassifiedAd")).toS("classified-ads");
    */
  }

  def objectType(`type`: String) =
    (key: String, event: JsonNode) => event("object")("@type").asText == `type`

  /**
    * Read the Kafka topic 'click-events' as strings and filter out
    * events that are invalid json. Send the correct events to the
    * topic 'json-events' as json.
    *
    * You can use the supplied 'tryParseJson' method to handle
    * parsing and error handling.
    */
  def filterOutInvalidJson(builder: StreamsBuilder): Unit = {
    val mapper = new ObjectMapper

    builder.stream("click-events", Consumed.`with`(strings, strings))
      .flatMapValuesS(tryParseJson)
      .toS("json-events")
  }

  private val mapper = new ObjectMapper

  def tryParseJson(event: String) = try {
    Seq(mapper.readTree(event))
  } catch {
    case NonFatal(ex) => Seq.empty[JsonNode]
  }

}
