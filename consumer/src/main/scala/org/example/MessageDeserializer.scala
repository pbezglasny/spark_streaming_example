package org.example

import org.apache.kafka.common.serialization.Deserializer
import org.exampe.InputMessage
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.read

class MessageDeserializer extends Deserializer[InputMessage] {

  implicit val formats = Serialization.formats(NoTypeHints)

  override def deserialize(topic: String, data: Array[Byte]): InputMessage = {
    read[InputMessage](new String(data, "UTF8"))
  }
}
