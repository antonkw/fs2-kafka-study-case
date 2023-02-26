package io.github.antonkw.demo

import cats.effect.{ ExitCode, IO, IOApp, Ref }
import cats.implicits.{ catsSyntaxEitherId, catsSyntaxOptionId, toFunctorOps }
import fs2.kafka._
import io.circe._
import io.circe.generic.extras.AutoDerivation
import io.github.antonkw.circe.adt.codec.JsonAdt

import java.util.UUID
import scala.util.control.NoStackTrace

import io.github.antonkw.circe.adt.codec._
import io.circe.syntax.EncoderOps


sealed trait AccountOperation {
  def account: UUID
}

object AccountOperation extends AutoDerivation {
  implicit val accountOperationDecoder: Decoder[AccountOperationV2] =
    JsonTaggedAdtCodec.createDecoder("operation_type")

  implicit val accountOperationEncoder: Encoder[AccountOperationV2] =
    JsonTaggedAdtCodec.createEncoder("operation_type")
}

@JsonAdt("withdrawal:v1")
final case class WithdrawalOperation(account: UUID, value: Long) extends AccountOperation

@JsonAdt("replenishment:v1")
final case class ReplenishmentOperation(account: UUID, value: Long) extends AccountOperation
object DeserializationDemoApp7 extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    def processRecord(
        record: ConsumerRecord[String, Either[Throwable, Input]]
    ): IO[Unit] =
      IO.println(record)

    type Input = AccountOperationV2
    sealed trait DeserializationError extends NoStackTrace
    case class InvalidJson(rawBody: String, cause: ParsingFailure) extends DeserializationError {
      override def toString: String =
        s"Invalid-json, [$rawBody] is not a json, cause: ${cause.getMessage}"
    }

    case class InvalidEntity(jsonBody: Json, cause: DecodingFailure) extends DeserializationError {
      override def toString: String =
        s"Invalid-entity, [${jsonBody.noSpaces}], cause: ${cause.getMessage()}]"
    }

    case class UnexpectedError(cause: Throwable) extends DeserializationError

    def deserializer[Body: Decoder]: Deserializer[IO, Either[DeserializationError, Body]] =
      Deserializer
        .string[IO]
        .flatMap(rawBody =>
          io.circe.parser
            .parse(rawBody)
            .fold(
              error => GenericDeserializer.fail[IO, Json](InvalidJson(rawBody, error)),
              GenericDeserializer.const[IO, Json]
            )
        )
        .flatMap(json =>
          json
            .as[Body]
            .fold(
              decodingFailure =>
                GenericDeserializer
                  .fail[IO, Body](InvalidEntity(json, decodingFailure)),
              GenericDeserializer.const[IO, Body]
            )
        )
        .attempt
        .map(_.left.map {
          case expected: DeserializationError => expected
          case unexpected                     => UnexpectedError(unexpected)
        })

    implicit val accountOperationDeserializer
        : Deserializer[IO, Either[DeserializationError, AccountOperationV2]] =
      deserializer[AccountOperationV2]

    val consumerSettings =
      ConsumerSettings[IO, String, Either[DeserializationError, Input]]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers("localhost:29092")
        .withGroupId("group")

    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers("localhost:29092")
        .withProperty("topic.creation.enable", "true")

    val produce = KafkaProducer
      .resource(producerSettings)
      .use(
        _.produce(
          ProducerRecords(
            List(
              ProducerRecord(
                "topic1",
                "key",
                """
                  |{
                  |  "operation_type" : "replenishment:v1" ,
                  |  "account": "123e4567-e89b-12d3-a456-426614174000",
                  |  "value": 200
                  |}
                  |""".stripMargin
              ),
              ProducerRecord(
                "topic1",
                "key",
                """
                  |{
                  | "operation_type" : "withdrawal:v1" ,
                  | "account": "123e4567-e89b-12d3-a456-426614174000",
                  | "value": 100
                  |}
                  |""".stripMargin
              )
            )
          )
        ).flatten
      )

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
