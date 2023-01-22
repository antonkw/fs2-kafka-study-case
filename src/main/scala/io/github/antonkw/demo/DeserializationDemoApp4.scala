package io.github.antonkw.demo

import cats.effect.{ExitCode, IO, IOApp}
import fs2.kafka._
import io.circe.{Decoder, Encoder, Json, JsonDecimal, JsonLong, JsonNumber, JsonObject}

object DeserializationDemoApp4 extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    def processRecord(record: ConsumerRecord[String, Either[Throwable, Input]]): IO[Unit] =
      IO.println(record)

    case class Example(value: Int)

    type Input = Example

    implicit object Example {
      implicit val encoder: Encoder[Example] = Encoder.forProduct1("value")(_.value)
      implicit val decoder: Decoder[Example] = Decoder.instance(_.get[Int]("value").map(Example.apply))
    }

    implicit def deserializer[A: Decoder] = Deserializer.string[IO]
      .map(io.circe.parser.parse)
      .flatMap(_.fold(GenericDeserializer.fail[IO, Json], GenericDeserializer.const[IO, Json]))
      .flatMap(_.as[A].fold(GenericDeserializer.fail[IO, A], GenericDeserializer.const[IO, A]))
      .attempt

    val consumerSettings =
      ConsumerSettings[IO, String, Either[Throwable, Input]]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:29092")
        .withGroupId("group")

    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost:29092")
        .withProperty("topic.creation.enable", "true")

    val produce = KafkaProducer
      .resource(producerSettings)
      .use(_.produceOne(ProducerRecord("topic1", "key", """{"value" : 42 }""")).flatten)

    val stream: fs2.Stream[IO, Unit] =
      KafkaConsumer
        .stream(consumerSettings)
        .subscribeTo("topic1")
        .records
        .evalMap { committable =>
          processRecord(committable.record)
        }

    produce *> stream.compile.drain.as(ExitCode.Success)
  }
}
