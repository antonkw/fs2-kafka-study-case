package io.github.antonkw.demo

import cats.effect.{ ExitCode, IO, IOApp }
import fs2.kafka._

object NaiveDecodingDemo extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    def processRecord(record: ConsumerRecord[String, String]): IO[Unit] =
      IO.println(record)

    type Input = String

    val consumerSettings =
      ConsumerSettings[IO, String, Input]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:29092")
        .withGroupId("group")

    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost:29092")
        .withProperty("topic.creation.enable", "true")

    val produce = KafkaProducer
      .resource(producerSettings)
      .use(_.produceOne(ProducerRecord("topic1", "key", "value")).flatten)

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
