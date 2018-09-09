package kafkastreams.scalaexercises

import org.apache.kafka.streams.scala.StreamsBuilder

class Exercise_3_Joins {

  /**
    * Join user events with account state based on user id, producing values of the form
    * <p>
    * pageId-accountState - e.g. 'pageA-closed'
    * <p>
    * For input and output topic names, see Exercise_3_JoinsTest
    */
  def accountStateJoin(builder: StreamsBuilder): Unit = ???

  /**
    * Join user events with account state, outputing the last visited page
    * when an account is cancelled/closed.
    */
  def accountCancellationLastVisitedPage(builder: StreamsBuilder): Unit = ???

  /**
    * Emit an event stream of account state changes paired with
    * each pageview happening within 1000 ms of the state change.
    */
  def pageViewsWithAccountStateChange(builder: StreamsBuilder): Unit = ???

  /**
    * Emit an event stream of account state changes paired with
    * a *list of* pageviews happening within 1000 ms of the state change.
    */
  def pageViewsWithListOfAccountStateChange(builder: StreamsBuilder): Unit = ???

}
