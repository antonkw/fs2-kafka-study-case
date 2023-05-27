package io.github.antonkw

import cats.effect.std.UUIDGen
import cats.effect.{ IO, Resource }
import cats.implicits._
import fs2.kafka._
import io.github.antonkw.demo.{ AccountOperationV2, BankState }
import io.github.antonkw.program.BankOperationProgram.deserializer
import io.github.antonkw.program.{ BankOperationProgram, DeserializationError }
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.IOSuite

import scala.concurrent.duration.DurationInt

object KafkaSuite extends weaver.IOSuite {
  lazy implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  case class TestResources(
      consumerSettings: ConsumerSettings[
        IO,
        String,
        Either[DeserializationError, AccountOperationV2]
      ],
      producerSettings: ProducerSettings[IO, String, String],
      inputTopic: String,
      outputTopic: String,
      consumer: KafkaConsumer[IO, String, String]
  )

  type Res = TestResources

  override def sharedResource: Resource[IO, Res] = {
    val consumerSettings = ConsumerSettings[IO, String, String]
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("localhost:29092")
      .withGroupId("group")

    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost:29092")
        .withProperty("topic.creation.enable", "true")

    KafkaConsumer
      .resource(consumerSettings)
      .product(Resource.eval(UUIDGen.randomUUID))
      .map { case (consumer, uuid) =>
        TestResources(
          consumerSettings =
            ConsumerSettings[IO, String, Either[DeserializationError, AccountOperationV2]]
              .withAutoOffsetReset(AutoOffsetReset.Earliest)
              .withBootstrapServers("localhost:29092")
              .withGroupId("group"),
          producerSettings = producerSettings,
          consumer = consumer,
          inputTopic = "input" + uuid.toString,
          outputTopic = "output" + uuid.toString
        )
      }
  }

  def produceTestMessages(
      producerSettings: ProducerSettings[IO, String, String],
      topic: String
  ): IO[ProducerResult[String, String]] = KafkaProducer
    .resource(producerSettings)
    .use(
      _.produce(
        ProducerRecords(
          List(
            ProducerRecord(
              topic,
              "key",
              """
                |{
                |  "operation_type" : "replenishment:v2" ,
                |  "account": "123e4567-e89b-12d3-a456-426614174000",
                |  "sequence_id" : 1,
                |  "value": 200
                |}
                |""".stripMargin
            ),
            ProducerRecord(
              topic,
              "key",
              """
                |{
                |  "operation_type" : "withdrawal:v2" ,
                |  "account": "123e4567-e89b-12d3-a456-426614174000",
                |  "sequence_id" : 1,
                |  "value": 100
                |}
                |""".stripMargin
            ),
            ProducerRecord(
              topic,
              "key",
              """
                |{
                |  "operation_type" : "withdrawal:v2" ,
                |  "account": "123e4567-e89b-12d3-a456-426614174000",
                |  "sequence_id" : 2,
                |  "value": 100
                |}
                |""".stripMargin
            )
          )
        )
      ).flatten
    )

  test("program produces expected messages") { resources =>
    val produce: IO[ProducerResult[String, String]] =
      produceTestMessages(resources.producerSettings, resources.inputTopic)

    val process: fs2.Stream[IO, Unit] = for {
      state <- fs2.Stream.eval(BankState.make)
      program = new BankOperationProgram(
        resources.consumerSettings,
        resources.producerSettings,
        resources.inputTopic,
        resources.outputTopic,
        state
      )
      _ <- program.process
    } yield ()

    for {
      started <- (produce *> process.compile.drain).start
      _       <- resources.consumer.subscribeTo(resources.outputTopic)
      received <- resources.consumer.records
        .take(3)
        .compile
        .toList
        .timeoutTo(5.seconds, started.cancel *> IO.raiseError(new RuntimeException("timeout")))
      _ <- started.cancel
    } yield expect(
      received.map(_.record.value) == List(
        "123e4567-e89b-12d3-a456-426614174000 updated, amount 200, sequence 1",
        "123e4567-e89b-12d3-a456-426614174000 was not updated, sequence 1, actual sequence 1",
        "123e4567-e89b-12d3-a456-426614174000 updated, amount 100, sequence 2"
      )
    )
  }
}
