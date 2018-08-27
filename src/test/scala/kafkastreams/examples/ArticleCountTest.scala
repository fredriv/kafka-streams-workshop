package kafkastreams.examples

import com.madewithtea.mockedstreams.MockedStreams

import kafkastreams.javaexamples.ArticleCountExample
//import kafkastreams.scalaexamples.ArticleCountExample
import org.scalatest.{FlatSpec, Matchers}

class ArticleCountTest extends FlatSpec with Matchers with ArticlesTestBase {

  "Article count example" should "count articles per site" in {
    // MockedStreams outputs every change in KTable
    val expected = List(
      "cnn" -> 1,
      "foxnews" -> 1,
      "bbc" -> 1,
      "cnn" -> 2,
      "bbc" -> 2,
      "cnn" -> 3,
      "foxnews" -> 2,
      "bbc" -> 3
    )

    val mstreams = MockedStreams()
      .topology(new ArticleCountExample().createTopology)
      .input("Articles", strings, json, articles)
      .output("ArticleCounts", strings, longs, expected.size) shouldEqual expected
  }
}
