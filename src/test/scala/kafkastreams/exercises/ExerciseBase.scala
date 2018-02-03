package kafkastreams.exercises

import org.scalatest.{FunSuite, Matchers}

trait ExerciseBase extends FunSuite with Matchers {

  // To solve exercises in Scala instead of Java, comment out the line
  // below and include the import for kafkastreams.scalaexercises._
  // instead

  import kafkastreams.javaexercises._
//  import kafkastreams.scalaexercises._

  val exercise1 = new Exercise_1_FilterAndTransform
  val exercise2 = new Exercise_2_Aggregations
}
