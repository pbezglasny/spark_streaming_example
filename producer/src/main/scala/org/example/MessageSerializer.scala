package org.example

import org.apache.kafka.common.serialization.Serializer
import org.exampe.InputMessage
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

class MessageSerializer extends Serializer[InputMessage] {

  implicit val formats = Serialization.formats(NoTypeHints)

  override def serialize(topic: String, data: InputMessage): Array[Byte] = {
    write(data).getBytes("UTF8")
  }
}
