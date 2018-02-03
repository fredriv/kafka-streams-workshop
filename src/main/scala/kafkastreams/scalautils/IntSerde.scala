package kafkastreams.scalautils

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

class IntSerde extends Serde[Int] {

  private val underlying = Serdes.Integer()

  override def serializer(): Serializer[Int] = new Serializer[Int] {
    override def serialize(topic: String, data: Int): Array[Byte] =
      underlying.serializer().serialize(topic, data)

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
    override def close(): Unit = {}
  }

  override def deserializer(): Deserializer[Int] = new Deserializer[Int] {
    override def deserialize(topic: String, data: Array[Byte]): Int =
      underlying.deserializer().deserialize(topic, data)

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
    override def close(): Unit = {}
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def close(): Unit = {}
}
