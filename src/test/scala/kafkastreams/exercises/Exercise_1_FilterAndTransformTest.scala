package kafkastreams.exercises

import com.fasterxml.jackson.databind.ObjectMapper
import com.madewithtea.mockedstreams.MockedStreams
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import ClickEvents.clickEvents

class Exercise_1_FilterAndTransformTest extends ExerciseBase {

  val strings = Serdes.String()
  val ints = Serdes.Integer()
  val json = new JsonNodeSerde

  val input = List(
    "Welcome to JavaZone 2018!",
    "September 11-13, 2018 Oslo",
    "Europe's biggest community-driven developer conference",
    "Located at the Oslo Spektrum conference centre"
  ).map((null, _))

  test("Events should flow directly through the Kafka Streams topology") {
    val expected = input

    val result = MockedStreams()
      .topology(builder => exercise1.passEventsThroughDirectly(toBuilder(builder)))
      .input("text", strings, strings, input)
      .output("pass-through", strings, strings, expected.size)

    result shouldEqual expected
  }

  test("Get length of lines") {
    val expected = List(25, 26, 54, 46)

    val result = MockedStreams()
      .topology(builder => exercise1.lineLengths(toBuilder(builder)))
      .input("text", strings, strings, input)
      .output("line-lengths", strings, ints, expected.size)

    result.map(_._2) shouldEqual expected
  }

  test("Get the number of words per line") {
    val expected = List(4, 4, 5, 7)

    val result = MockedStreams()
      .topology(builder => exercise1.wordsPerLine(toBuilder(builder)))
      .input("text", strings, strings, input)
      .output("words-per-line", strings, ints, expected.size)

    result.map(_._2) shouldEqual expected
  }

  test("Get the lines containing 'conference'") {
    val expected = List(
      "Europe's biggest community-driven developer conference",
      "Located at the Oslo Spektrum conference centre"
    )

    val result = MockedStreams()
      .topology(builder => exercise1.linesContainingConference(toBuilder(builder)))
      .input("text", strings, strings, input)
      .output("contains-conference", strings, strings, expected.size)

    result.map(_._2) shouldEqual expected
  }

  test("Get all the words") {
    val expected = List(
      "Welcome", "to", "JavaZone", "2018!",
      "September", "11-13,", "2018", "Oslo",
      "Europe's", "biggest", "community-driven", "developer", "conference",
      "Located", "at", "the", "Oslo", "Spektrum", "conference", "centre"
    )

    val result = MockedStreams()
      .topology(builder => exercise1.allTheWords(toBuilder(builder)))
      .input("text", strings, strings, input)
      .output("all-the-words", strings, strings, expected.size)

    result.map(_._2) shouldEqual expected
  }

  test("Find URLs of visited pages") {
    val expected = List(
      "https://www.finn.no/bap/forsale/ad.html?finnkode=109312065",
      "https://www.aftenposten.no/verden/i/0EjoyJ/Trump-vil-fore-opp-Nord-Korea-pa-terrorlisten",
      "https://www.blocket.se/stockholm/Bianchi_aria_76294293.htm?ca=11&w=1",
      "https://www.leboncoin.fr/jeux_jouets/1310126037.htm?ca=7_s",
      "https://www.aftonbladet.se/nyheter/a/J1WkPX/vulkanen-ryker--flyg-stalls-in",
      "https://www.finn.no/bap/webstore/ad.html?finnkode=107329093",
      "https://www.finn.no/bap/forsale/ad.html?finnkode=105004553"
    )

    val result = MockedStreams()
      .topology(builder => exercise1.urlsVisited(toBuilder(builder)))
      .input("click-events", strings, json, clickEvents)
      .output("urls-visited", strings, strings, expected.size)

    result.map(_._2) shouldEqual expected
  }

  test("Find Click events for articles") {
    val expected = List(clickEvents(1), clickEvents(4))

    val result = MockedStreams()
      .topology(builder => exercise1.articles(toBuilder(builder)))
      .input("click-events", strings, json, clickEvents)
      .output("articles", strings, json, expected.size)

    result shouldEqual expected
  }

  test("Find URLs of visited articles") {
    val expected = List(
      "https://www.aftenposten.no/verden/i/0EjoyJ/Trump-vil-fore-opp-Nord-Korea-pa-terrorlisten",
      "https://www.aftonbladet.se/nyheter/a/J1WkPX/vulkanen-ryker--flyg-stalls-in"
    )

    val result = MockedStreams()
      .topology(builder => exercise1.articleVisits(toBuilder(builder)))
      .input("click-events", strings, json, clickEvents)
      .output("article-urls", strings, strings, expected.size)

    result.map(_._2) shouldEqual expected
  }

  test("Find prices of visited classified ads") {
    val expected = List(1500, 23000, 1, 198, 500)

    val result = MockedStreams()
      .topology(builder => exercise1.classifiedAdPrices(toBuilder(builder)))
      .input("click-events", strings, json, clickEvents)
      .output("classified-ad-prices", strings, ints, expected.size)

    result.map(_._2) shouldEqual expected
  }

  test("Create simplified classified ads") {
    val mapper = new ObjectMapper()
    val expected = List(
      """{ "title": "Klassisk DBS selges", "price": 1500 }""",
      """{ "title": "Bianchi aria", "price": 23000 }""",
      """{ "title": "Divers jeux d'éveil et hochets", "price": 1 }""",
      """{ "title": "Tica Discovery Fly. Stang for fluefiske for et par hundrelapper pga vannskade", "price": 198 }""",
      """{ "title": "Vadebukse", "price": 500 }"""
    ).map(mapper.readTree)

    val result = MockedStreams()
      .topology(builder => exercise1.simplifiedClassifiedAds(toBuilder(builder)))
      .input("click-events", strings, json, clickEvents)
      .output("simplified-classified-ads", strings, json, expected.size)

    result.map(_._2) shouldEqual expected
  }

  test("Split articles and ads into substreams") {
    val expectedArticles = List(clickEvents(1), clickEvents(4))
    val expectedAds = List(clickEvents(0), clickEvents(2), clickEvents(3), clickEvents(5), clickEvents(6))

    val streams = MockedStreams()
      .topology(builder => exercise1.splitArticlesAndAds(toBuilder(builder)))
      .input("click-events", strings, json, clickEvents)

    val articles = streams.output("articles", strings, json, expectedArticles.size)
    val ads = streams.output("classified-ads", strings, json, expectedAds.size)

    articles shouldEqual expectedArticles
    ads shouldEqual expectedAds
  }

  test("Filter our garbage events") {
    val expected = clickEvents
    val input = (null, "invalid json") :: clickEvents.map { case (k, v) => (k, v.toString) }

    val result =  MockedStreams()
      .topology(builder => exercise1.filterOutInvalidJson(toBuilder(builder)))
      .input("click-events", strings, strings, input)
      .output("json-events", strings, json, expected.size)

    result shouldEqual expected
  }

}
