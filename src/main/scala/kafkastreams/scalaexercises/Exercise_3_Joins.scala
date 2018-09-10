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
    val accountStateKTable = builder.table[String, String]("account-state-changelog")
    val userEventStream = builder.stream[String, String]("user-events")

    userEventStream
      .join(accountStateKTable)((value1, value2) => value1 + "-" + value2)
      .to("user-events-with-accountstate")
  }

  /**
    * Join user events with account state, outputing the last visited page
    * when an account is cancelled/closed.
    */
  def accountCancellationLastVisitedPage(builder: StreamsBuilder): Unit = {
    val accountState = builder.stream[String, String]("account-state-changelog")
    val userEventStream = builder.table[String, String]("user-events")

    accountState
      .filter((key, value) => value == "closed")
      .join(userEventStream)((value1, value2) => value2)
      .to("account-cancellation-last-visited-page")
  }

  def createPageViewRelatedToAccountChangeJoiner(mapper: ObjectMapper) =
    (accountState: JsonNode, pageView: JsonNode) => getPageViewRelatedToAccountChange(mapper, accountState, pageView)

  def getPageViewRelatedToAccountChange(mapper: ObjectMapper, accountState: JsonNode, pageView: JsonNode): JsonNode = {
    val node = mapper.createObjectNode

    node.set("pageTimestamp", pageView.path("timestamp"))
    node.set("stateTimestamp", accountState.path("timestamp"))
    node.set("page", pageView.path("page"))
    node.set("state", accountState.path("state"))

    node
  }

  /**
    * Emit an event stream of account state changes paired with
    * each pageview happening within 1000 ms of the state change.
    *
    * You can fill in and use the helper methods above.
    */
  def pageViewsWithAccountStateChange(builder: StreamsBuilder): Unit = {
    val accountStateStream = builder.stream[String, JsonNode]("account-state-changelog")
    val userEventStream = builder.stream[String, JsonNode]("user-events")

    val mapper = new ObjectMapper

    accountStateStream
      .join(userEventStream)(
        createPageViewRelatedToAccountChangeJoiner(mapper),
        JoinWindows.of(1000)
      )
      .to("account-state-coinciding-pageview")
  }

  /**
    * Emit an event stream of account state changes paired with
    * a *list of* pageviews happening within 1000 ms of the state change.
    */
  def pageViewsWithListOfAccountStateChange(builder: StreamsBuilder): Unit = {
    val accountStateStream = builder.stream[String, JsonNode]("account-state-changelog")
    val userEventStream = builder.stream[String, JsonNode]("user-events")

    val mapper = new ObjectMapper

    val aggregator = (key: String, value: JsonNode, aggregate: JsonNode) => {
      val page = mapper.createObjectNode
      page.set("id", value.path("page"))
      page.set("timestamp", value.path("pageTimestamp"))
      aggregate.path("pages").asInstanceOf[ArrayNode].add(page)
      aggregate.asInstanceOf[ObjectNode].set("state", value.path("state"))
      aggregate.asInstanceOf[ObjectNode].set("stateTimestamp", value.path("stateTimestamp"))
      aggregate
    }

    def initializer: JsonNode = {
      val result = mapper.createObjectNode
      result.set("pages", mapper.createArrayNode)
    }

    accountStateStream
      .join(userEventStream)(
        createPageViewRelatedToAccountChangeJoiner(mapper),
        JoinWindows.of(1000)
      )
      .groupBy((key, value) => key + "-" + value.path("state").asText + "-" + value.path("stateTimestamp").toString) // Gotcha - what happens if you use .textValue here?
      .aggregate(initializer)(aggregator)
      .toStream
      .to("account-state-coinciding-pageslist")
  }

}
