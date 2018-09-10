package kafkastreams.scalaexercises

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala.{StreamsBuilder, kstream}

class Exercise_3_Joins {

  private implicit val json = new JsonNodeSerde

  /**
    * Join user events with account state based on user id, producing values of the form
    * <p>
    * pageId-accountState - e.g. 'pageA-closed'
    * <p>
    * For input and output topic names, see Exercise_3_JoinsTest
    */
  def accountStateJoin(builder: StreamsBuilder): Unit = {

  }

  /**
    * Join user events with account state, outputing the last visited page
    * when an account is cancelled/closed.
    */
  def accountCancellationLastVisitedPage(builder: StreamsBuilder): Unit = {

  }

  def createPageViewRelatedToAccountChangeJoiner(mapper: ObjectMapper) =
    (accountState: JsonNode, pageView: JsonNode) => getPageViewRelatedToAccountChange(mapper, accountState, pageView)

  def getPageViewRelatedToAccountChange(mapper: ObjectMapper, accountState: JsonNode, pageView: JsonNode): JsonNode = {
    val node = mapper.createObjectNode

    // TODO populate the JSON object with the expected fields. See Exercise_3_JoinsTest for details.

    node
  }

  /**
    * Emit an event stream of account state changes paired with
    * each pageview happening within 1000 ms of the state change.
    *
    * You can fill in and use the helper methods above.
    */
  def pageViewsWithAccountStateChange(builder: StreamsBuilder): Unit = {

  }

  /**
    * Emit an event stream of account state changes paired with
    * a *list of* pageviews happening within 1000 ms of the state change.
    */
  def pageViewsWithListOfAccountStateChange(builder: StreamsBuilder): Unit = {

  }

}
