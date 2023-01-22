package io.github.antonkw.demo

import cats.effect.{ ExitCode, IO, IOApp }
import fs2.kafka._
import io.circe._

import java.util.UUID
import scala.util.control.NoStackTrace

object DeserializationDemoApp5 extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    def processRecord(
        record: ConsumerRecord[String, Either[Throwable, Input]]
    ): IO[Unit] =
      IO.println(record)

    case class Example(value: Int) {
      def traceable(correlationId: UUID) = TraceableExample(value, correlationId)
    }v

    case class TraceableExample(value: Int, correlationId: UUID) extends Traceable

    type Input = TraceableExample

    implicit object Example {
      implicit val encoder: Encoder[Example] = Encoder.forProduct1("value")(_.value)
      implicit val decoder: Decoder[Example] =
        Decoder.instance(_.get[Int]("value").map(Example.apply))
    }

    val CorrelationId = "correlation_id"
    sealed trait DeserializationError extends NoStackTrace

    case class NoHeaderFound(name: String, headers: Headers) extends DeserializationError {
      override def toString: String =
        s"Header-not-found, headers[${headers.toChain.toList.map(_.key).mkString(",")}]"
    }

    case class HeaderDeserializationError(name: String, header: Header, e: Throwable)
        extends DeserializationError {
      override def toString: String =
        s"Header-deserialization-error, header:[${header.key()}->${header.as[String]}], cause: [${e.getMessage}]"
    }

    case class InvalidJson(rawBody: String, cause: ParsingFailure) extends DeserializationError {
      override def toString: String =
        s"Invalid-json, [$rawBody] is not a json, cause: ${cause.getMessage}"
    }

    case class InvalidEntity(jsonBody: Json, cause: DecodingFailure) extends DeserializationError {
      override def toString: String =
        s"Invalid-entity, [${jsonBody.noSpaces}], cause: ${cause.getMessage()}]"
    }

    case class UnexpectedError(cause: Throwable) extends DeserializationError

    trait Traceable {
      def correlationId: UUID
    }

    def deserializer[Body: Decoder, Rich <: Traceable](
        f: (Body, UUID) => Rich
    ): Deserializer[IO, Either[DeserializationError, Rich]] =
      Deserializer
        .headers(headers =>
          headers(CorrelationId)
            .map(header =>
              header
                .attemptAs[UUID]
                .fold(
                  error =>
                    GenericDeserializer
                      .fail[IO, UUID](
                        HeaderDeserializationError(CorrelationId, header, error)
                      ),
                  GenericDeserializer.const[IO, UUID]
                )
            )
            .getOrElse(
              GenericDeserializer.fail[IO, UUID](NoHeaderFound(CorrelationId, headers))
            )
            .flatMap(correlationId =>
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
                .map(f(_, correlationId))
            )
        )
        .attempt
        .map(_.left.map {
          case expected: DeserializationError => expected
          case unexpected                     => UnexpectedError(unexpected)
        })

    implicit val traceableExampleDeserializer
        : Deserializer[IO, Either[DeserializationError, TraceableExample]] =
      deserializer[Example, TraceableExample](_ traceable _)
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
        _.produceOne(
          ProducerRecord("topic1", "key", """ { "value" : 42 } """)
            .withHeaders(
              Headers(Header[String]("correlation_id", "123e4567-e89b-12d3-a456-426614174000"))
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
