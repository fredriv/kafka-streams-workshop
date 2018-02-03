package kafkastreams.scalautils

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KGroupedStream, KStream, Predicate, Produced, Serialized}

import scala.collection.JavaConverters._

/**
  * DSL to improve Kafka Streams usage from Scala.
  *
  * By using the DSL, you no longer need to specify the key and value
  * Serdes with Produced or Serialized if the necessary Serdes are
  * implicitly available.
  *
  * The other methods allow the use of regular Scala functions in place
  * of ValueMapper, Predicate etc., and also remove the need to specify
  * the result value type explicitly.
  *
  * Finally, the DSL adds new symbols that can be used in place of the
  * methods:
  *
  *   '\' instead of filter,
  *   '~>' instead of mapS, mapValuesS, and toS, and
  *   '~>>' instead of flatMapValuesS
  */
object KafkaStreamsDSL {

  implicit class RichKStream[K, V](stream: KStream[K, V]) {
    def toS(topic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit =
      stream.to(topic, Produced.`with`(keySerde, valSerde))

    def ~>(topic: String)(implicit keySerde: Serde[K], valSerde: Serde[V]): Unit = toS(topic)

    def \(filter: V => Boolean): KStream[K, V] =
      stream.filter((key, value) => filter(value))

    def \(filter: (K, V) => Boolean): KStream[K, V] =
      stream.filter((key, value) => filter(key, value))

    def mapValuesS[VR](mapper: V => VR): KStream[K, VR] =
      stream.mapValues[VR](mapper(_))

    def ~>[VR](mapper: V => VR): KStream[K, VR] = mapValuesS(mapper)

    def mapS[KR, VR](mapper: (K, V) => (KR, VR)): KStream[KR, VR] =
      stream.map[KR, VR]((k: K, v: V) => mapper(k, v) match {
        case (newKey, newValue) => KeyValue.pair(newKey, newValue)
      })

    def ~>[KR, VR](mapper: (K, V) => (KR, VR)): KStream[KR, VR] = mapS(mapper)

    def flatMapValuesS[VR](mapper: V => Seq[VR]): KStream[K, VR] =
      stream.flatMapValues[VR](value => mapper(value).asJava)

    def ~>>[VR](mapper: V => Seq[VR]): KStream[K, VR] = flatMapValuesS(mapper)

    def groupByKeyS(implicit keySerde: Serde[K], valSerde: Serde[V]): KGroupedStream[K, V] =
      stream.groupByKey(Serialized.`with`(keySerde, valSerde))

    def groupByS[KR](mapper: (K, V) => KR)(implicit keySerde: Serde[KR], valSerde: Serde[V]): KGroupedStream[KR, V] =
      stream.groupBy((key: K, value: V) => mapper(key, value), Serialized.`with`(keySerde, valSerde))

    def branchS(predicates: ((K, V) => Boolean)*): Seq[KStream[K, V]] = {
      def toPredicate(p: (K, V) => Boolean): Predicate[K, V] = p(_, _)
      stream.branch(predicates.map(toPredicate): _*).toList
    }
  }

}
