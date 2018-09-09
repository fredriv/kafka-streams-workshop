package kafkastreams.exercises

import com.madewithtea.mockedstreams.MockedStreams
import org.apache.kafka.common.serialization.Serdes

class Exercise_0_HelloKafkaStreamsTest extends ExerciseBase {

  val strings = Serdes.String()

  val input = List(
    "Welcome to JavaZone 2018!",
    "September 11-13, 2018 Oslo",
    "Europe's biggest community-driven developer conference",
    "Located at the Oslo Spektrum conference centre"
  ).map((null, _))

  test("Input should be forwarded to new topic") {
    val expected = input

    val result = MockedStreams()
      .topology(builder => exercise0.passEventsThroughDirectly(toBuilder(builder)))
      .input("text", strings, strings, input)
      .output("pass-through", strings, strings, expected.size)

    result shouldEqual expected
  }

}
