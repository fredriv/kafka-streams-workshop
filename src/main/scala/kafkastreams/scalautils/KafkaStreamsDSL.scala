package kafkastreams.scalautils

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KGroupedStream, KStream, Produced, Serialized}

/**
  * DSL to improve Kafka Streams usage from Scala.
  *
  * By using the DSL, you no longer need to specify the key and value
  * Serdes using Produced.`with` or Serialized.`with` if the necessary
  * Serdes are implicitly available.
  *
  * Also, the mapS and mapValuesS remove the need to specify the
  * result value type explicitly.
  */
object KafkaStreamsDSL {

  implicit class RichKStream[K, V](stream: KStream[K, V]) {
    def toS(topic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit =
      stream.to(topic, Produced.`with`(keySerde, valSerde))

    def mapValuesS[VR](mapper: V => VR): KStream[K, VR] =
      stream.mapValues[VR]((v: V) => mapper(v))

    def mapS[KR, VR](mapper: (K, V) => (KR, VR)): KStream[KR, VR] =
      stream.map[KR, VR]((k: K, v: V) => mapper(k, v) match {
        case (newKey, newValue) => KeyValue.pair(newKey, newValue)
      })

    def groupByKeyS(implicit keySerde: Serde[K], valSerde: Serde[V]): KGroupedStream[K, V] =
      stream.groupByKey(Serialized.`with`(keySerde, valSerde))

    def groupByS[KR](mapper: (K, V) => KR)(implicit keySerde: Serde[KR], valSerde: Serde[V]): KGroupedStream[KR, V] =
      stream.groupBy((key: K, value: V) => mapper(key, value), Serialized.`with`(keySerde, valSerde))
  }

}
