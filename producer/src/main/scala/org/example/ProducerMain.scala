package org.example

import java.net.URLEncoder
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.exampe.InputMessage
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Random, Success}

object ProducerMain {

  val topic = "input_topic"
  implicit val context: ExecutionContextExecutor = ExecutionContext.global

  private val logger = LoggerFactory.getLogger(this.getClass)

  val random = new Random(42)

  private val hosts = Array("example.org", "google.com", "yandex.ru")
  private val paths = Array("path", "path with spaces", "путь", "путь с пробелом")

  def randomMessage: String = {
    s"http://${hosts(random.nextInt(hosts.length))}/${paths(random.nextInt(paths.length))}"
  }

  def getMessage: InputMessage = {
    InputMessage(
      s"${random.nextInt(15)}-${random.nextInt(5)}-${random.nextInt(10)}",
      s"792912345${random.nextInt(10)}",
      "Deliver",
      s"${random.nextInt(9)}",
      s"${random.nextInt()}",
      URLEncoder.encode(randomMessage, "UTF8"),
      LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    val producer = new KafkaProducer[String, InputMessage](props, new StringSerializer(), new MessageSerializer())
    val thread = new Thread(() => {
      for (_ <- 1 to 1000) {
        val record = new ProducerRecord[String, InputMessage](topic, getMessage)
        val javaFuture = producer.send(record)
        Future(javaFuture.get()).onComplete {
          case Success(value) => logger.info(value.toString)
          case Failure(exception) => logger.error(exception.getMessage)
        }
        Thread.sleep(500)
      }
    })
    thread.start()
    thread.join()
  }
}
