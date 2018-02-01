package kafkastreams.scalaexercises

import java.util
import java.util.Collections

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Predicate, Produced, ValueMapper}
import org.apache.kafka.streams.{Consumed, StreamsBuilder}

import scala.util.control.NonFatal

object Exercise_1_FilterAndTransform {

  private val strings = Serdes.String
  private val ints = Serdes.Integer
  private val json = new JsonNodeSerde

  /**
    * Read the Kafka topic 'text' and send the contents directly to
    * the new topic 'pass-through'
    */
  def passEventsThroughDirectly(builder: StreamsBuilder): Unit = {
    val stream = builder.stream("text", Consumed.`with`(strings, strings))
    stream.to("pass-through", Produced.`with`(strings, strings))
  }

  /**
    * Read the Kafka topic 'text', convert each line of text to the
    * length of that text and send it to the topic 'line-lengths'
    * as a stream of ints
    */
  def lineLengths(builder: StreamsBuilder): Unit = {
    builder.stream("text", Consumed.`with`(strings, strings))
      .mapValues[Integer](line => line.length)
      .to("line-lengths", Produced.`with`(strings, ints))
  }

  /**
    * Read the Kafka topic 'text', count the number of words in
    * each line and send that to the topic 'words-per-line' as a
    * stream of ints
    */
  def wordsPerLine(builder: StreamsBuilder): Unit = {
    builder.stream("text", Consumed.`with`(strings, strings))
      .mapValues[Integer](line => line.split(" ").length)
      .to("words-per-line", Produced.`with`(strings, ints))
  }

  /**
    * Read the Kafka topic 'text', find the lines containing the
    * word 'conference' and send them to the topic
    * 'contains-conference'
    */
  def linesContainingData(builder: StreamsBuilder): Unit = {
    builder.stream("text", Consumed.`with`(strings, strings))
      .filter((key, line) => line.contains("conference"))
      .to("contains-conference", Produced.`with`(strings, strings))
  }

  /**
    * Read the Kafka topic 'text', split each line into words and
    * send them individually to the topic 'all-the-words'
    */
  def allTheWords(builder: StreamsBuilder): Unit = {
    builder.stream("text", Consumed.`with`(strings, strings))
      .flatMapValues[String](line => util.Arrays.asList(line.split(" "): _*))
      .to("all-the-words", Produced.`with`(strings, strings))
  }

  /**
    * Read the Kafka topic 'click-events' as json, get the object URL
    * (see 'ClickEvents' class in the 'testdata' package for details)
    * and send the URL as a string to the topic 'urls-visited'
    */
  def urlsVisited(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .mapValues[String](json => json.path("object").path("url").asText)
      .to("urls-visited", Produced.`with`(strings, strings))
  }

  /**
    * Read the Kafka topic 'click-events' as json, find the events
    * that are for objects of @type 'Article' (see 'ClickEvents'
    * class in the 'testdata' package for details) and send the
    * events unmodified to the topic 'articles' as json
    */
  def articles(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .filter((key, json) => json.path("object").path("@type").asText == "Article")
      .to("articles", Produced.`with`(strings, json))
  }

  /**
    * Read the Kafka topic 'click-events' as json, find the events
    * that are for objects of @type 'Article' and send the object
    * URLs to the topic 'article-urls' as strings
    */
  def articleVisits(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .filter((key, json) => json.path("object").path("@type").asText == "Article")
      .mapValues[String](json => json.path("object").path("url").asText)
      .to("article-urls", Produced.`with`(strings, strings))
  }

  /**
    * Read the Kafka topic 'click-events' as json, find the events
    * that are for objects of @type 'ClassifiedAd' and send the
    * object prices to the topic 'classified-ad-prices' as ints
    */
  def classifiedAdPrices(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .filter((key, json) => json.path("object").path("@type").asText == "ClassifiedAd")
      .mapValues[Integer](json => json.path("object").path("price").asInt)
      .to("classified-ad-prices", Produced.`with`(strings, ints))
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

    val simplifiedClassifiedAd: ValueMapper[JsonNode, JsonNode] =
      json => mapper.createObjectNode
        .put("title", json.path("object").path("name").asText())
        .put("price", json.path("object").path("price").asInt())

    builder.stream("click-events", Consumed.`with`(strings, json))
      .filter((key, json) => json.path("object").path("@type").asText == "ClassifiedAd")
      .mapValues[JsonNode](simplifiedClassifiedAd)
      .to("simplified-classified-ads", Produced.`with`(strings, json))
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
    val branches = clicks.branch(objectType("Article"), objectType("ClassifiedAd"))
    branches(0).to("articles", Produced.`with`(strings, json))
    branches(1).to("classified-ads", Produced.`with`(strings, json))

    /*
    clicks.filter(objectType("Article")).to(strings, json, "articles");
    clicks.filter(objectType("ClassifiedAd")).to(strings, json, "classified-ads");
    */
  }

  def objectType(`type`: String): Predicate[String, JsonNode] =
    (key, json) => json.path("object").path("@type").asText == `type`

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
      .flatMapValues[JsonNode](tryParseJson)
      .to("json-events", Produced.`with`(strings, json))
  }

  private val mapper = new ObjectMapper

  def tryParseJson(event: String) = try {
    Collections.singletonList(mapper.readTree(event))
  } catch {
    case NonFatal(ex) => Collections.emptyList
  }

}
