package kafkastreams.scalautils

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.kstream.KStream

/**
  * This DSL adds new symbols that can be used in place of the
  * regular KStream methods:
  *
  *   '\' instead of filter,
  *   '~>' instead of map, mapValues, and to, and
  *   '~>>' instead of flatMapValues
  */
object KafkaStreamsDSL {

  implicit class RichKStream[K, V](stream: KStream[K, V]) {
    def ~>(topic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit =
      stream.to(topic)

    def \(filter: V => Boolean): KStream[K, V] =
      stream.filter((key, value) => filter(value))

    def ~>[VR](mapper: V => VR): KStream[K, VR] =
      stream.mapValues(mapper)

    def ~>[KR, VR](mapper: (K, V) => (KR, VR)): KStream[KR, VR] =
      stream.map(mapper)

    def ~>>[VR](mapper: V => Iterable[VR]): KStream[K, VR] =
      stream.flatMapValues(mapper)
  }
}
