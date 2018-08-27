package kafkastreams.examples

import com.madewithtea.mockedstreams.MockedStreams

import kafkastreams.javaexamples.BranchExample
//import kafkastreams.scalaexamples.BranchExample
import org.scalatest.{FlatSpec, Matchers}

class BranchTest extends FlatSpec with Matchers with ArticlesTestBase {

  "Branch example" should "route articles to site topic" in {
    val bbcArticles = List(2, 4, 7).map(articles)
    val cnnArticles = List(0, 3, 5).map(articles)
    val foxArticles = List(1, 6).map(articles)

    val mstreams = MockedStreams()
      .topology(new BranchExample().createTopology)
      .input("Articles", strings, json, articles)

    mstreams.output("BBC-Articles", strings, json, bbcArticles.size) shouldBe bbcArticles
    mstreams.output("CNN-Articles", strings, json, cnnArticles.size) shouldBe cnnArticles
    mstreams.output("FoxNews-Articles", strings, json, foxArticles.size) shouldBe foxArticles
  }
}
