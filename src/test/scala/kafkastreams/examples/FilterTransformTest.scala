package kafkastreams.examples

import com.madewithtea.mockedstreams.MockedStreams

import kafkastreams.javaexamples.FilterTransformExample
//import kafkastreams.scalaexamples.FilterTransformExample
import org.scalatest.{FlatSpec, Matchers}

class FilterTransformTest extends FlatSpec with Matchers with ArticlesTestBase {

  "Filter and transform example" should "send BBC articles to 'BBC-Articles' topic" in {
    val bbcArticles = List(2, 4, 7).map(articles)

    MockedStreams()
      .topology(new FilterTransformExample().createTopology)
      .input("Articles", strings, json, articles)
      .output("BBC-Articles", strings, json, bbcArticles.size) shouldBe bbcArticles
  }

  it should "send BBC article titles to 'BBC-Titles' topic" in {
    val expected = List(
      ("3", "Employees urged to let staff 'rest'"),
      ("5", "What to watch for in Trump's SOTU speech"),
      ("8", "The truth about the origin of macaroni cheese")
    )

    MockedStreams()
      .topology(new FilterTransformExample().createTopology)
      .input("Articles", strings, json, articles)
      .output("BBC-Titles", strings, strings, expected.size) shouldBe expected
  }
}
