package kafkastreams.exercises

import org.scalatest.{FunSuite, Matchers}
import org.apache.kafka.streams.{StreamsBuilder => StreamsBuilderJ}
import org.apache.kafka.streams.scala.StreamsBuilder

trait ExerciseBase extends FunSuite with Matchers {

  // To solve exercises in Scala instead of Java, comment out the import
  // line and toBuilder method below and include the Scala versions instead

  import kafkastreams.javaexercises._
  def toBuilder(builder: StreamsBuilderJ) = builder

//  import kafkastreams.scalaexercises._
//  def toBuilder(builder: StreamsBuilderJ) = new StreamsBuilder(builder)

  val exercise1 = new Exercise_1_FilterAndTransform
  val exercise2 = new Exercise_2_Aggregations
  val exercise3 = new Exercise_3_Joins

}
