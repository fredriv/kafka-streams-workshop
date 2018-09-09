package kafkastreams.examples

import com.madewithtea.mockedstreams.MockedStreams
import kafkastreams.javaexamples.HelloKafkaStreams
//import kafkastreams.scalaexamples.HelloKafkaStreams
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.scalatest.{FlatSpec, Matchers}

class HelloKafkaStreamsTest extends FlatSpec with Matchers {

  val strings = new StringSerde

  "The Hello World application" should "send greetings to 'hello' topic" in {
    val input = List("" -> "JavaZone")
    val expected = List("" -> "Hello, JavaZone!")

    MockedStreams()
      .topology(new HelloKafkaStreams().createTopology)
      .input("names", strings, strings, input)
      .output("hello", strings, strings, expected.size) shouldBe expected
  }
}
