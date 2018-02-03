package kafkastreams.scalautils

import com.fasterxml.jackson.databind.JsonNode

/**
  * DSL to improve Jackson usage from Scala.
  *
  * Let's you navigate a JSON structure like
  *
  *   json \ "fieldName" \ "otherField"
  *
  * or
  *
  *   json("fieldName")("otherField")(arrayIndex)
  */
object JacksonDSL {

  implicit class RichJsonNode(json: JsonNode) {
    def \(fieldName: String) = json.path(fieldName)
    def apply(fieldName: String) = json.path(fieldName)
    def apply(index: Int) = json.path(index)
  }

}
