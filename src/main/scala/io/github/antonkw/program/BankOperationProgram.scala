package io.github.antonkw.program

import cats.effect.{ ExitCode, IO, IOApp, Ref }
import cats.implicits.{ catsSyntaxEitherId, catsSyntaxOptionId, toFunctorOps }
import fs2.Pipe
import fs2.kafka.KafkaProducer.pipe
import fs2.kafka.{ ConsumerRecord, ProducerRecords, _ }
import io.circe._
import io.circe.generic.extras.AutoDerivation
import io.github.antonkw.circe.adt.codec.JsonAdt

import java.util.UUID
import scala.util.control.NoStackTrace
import io.github.antonkw.circe.adt.codec._
import io.circe.syntax.EncoderOps
import io.github.antonkw.demo.{
  AccountOperationResult,
  AccountOperationV2,
  BankError,
  BankState,
  ReplenishmentOperationV2,
  WithdrawalOperationV2
}

import scala.concurrent.duration.DurationInt
import scala.util.Either
import scala.util.chaining.scalaUtilChainingOps
import io.github.antonkw.demo.{
  AccountNotFound,
  AccountUpdated,
  InsufficientAmount,
  OperationIgnored
}

class BankOperationProgram(
    consumerSettings: ConsumerSettings[
      IO,
      String,
      Either[DeserializationError, AccountOperationV2]
    ],
    producerSettings: ProducerSettings[IO, String, String],
    inputTopic: String,
    outputTopic: String,
    state: BankState
) {
  def processRecord(bankState: BankState)(
      record: ConsumerRecord[String, Either[Throwable, AccountOperationV2]]
  ): IO[Unit] =
    record.value match {
      case Left(value) => IO.println(value)
      case Right(operation) =>
        operation match {
          case op: WithdrawalOperationV2    => bankState.withdraw(op).flatMap(IO.println)
          case op: ReplenishmentOperationV2 => bankState.replenish(op).flatMap(IO.println)
        }
    }

  implicit val accountOperationDeserializer
      : Deserializer[IO, Either[DeserializationError, AccountOperationV2]] =
    BankOperationProgram.deserializer[AccountOperationV2]

  val collectValidValues: Pipe[
    IO,
    CommittableConsumerRecord[IO, String, Either[DeserializationError, AccountOperationV2]],
    CommittableConsumerRecord[IO, String, AccountOperationV2]
  ] = _.fproduct(_.record.value).collect { case (committable, Right(validValue)) =>
    committable.as(validValue)
  }

  val collectFailures: Pipe[
    IO,
    CommittableConsumerRecord[IO, String, Either[DeserializationError, AccountOperationV2]],
    CommittableConsumerRecord[IO, String, DeserializationError]
  ] = _.fproduct(_.record.value).collect { case (committable, Left(failure)) =>
    committable.as(failure)
  }

  def updateBankState(state: BankState): Pipe[
    IO,
    CommittableConsumerRecord[IO, String, AccountOperationV2],
    CommittableConsumerRecord[IO, String, String]
  ] = _.evalMap { case committable @ CommittableConsumerRecord(record, _) =>
    val resultIO: IO[Either[Throwable, Either[BankError, AccountOperationResult]]] =
      record.value match {
        case withdrawal: WithdrawalOperationV2 =>
          state.withdraw(withdrawal).attempt
        case replenishment: ReplenishmentOperationV2 =>
          state.replenish(replenishment).map(_.asRight[BankError]).attempt
      }

    val message: IO[String] = resultIO.map {
      case Left(exception) => s"exception ${exception.getMessage}"
      case Right(operationResult) =>
        operationResult match {
          case Left(bankError: BankError) =>
            bankError match {
              case AccountNotFound(accountId)            => s"not found $accountId"
              case InsufficientAmount(accountId, actual) => s"$accountId: lack of money $actual"
            }
          case Right(value) =>
            value match {
              case AccountUpdated(accountId, amount, sequenceId) =>
                s"$accountId updated, amount $amount, sequence $sequenceId"
              case OperationIgnored(accountId, sequenceId, actualSequence) =>
                s"$accountId was not updated, sequence $sequenceId, actual sequence $actualSequence"
            }
        }
    }

    message.map(committable.as)
  }

  val produceStatus: Pipe[IO, CommittableConsumerRecord[IO, String, String], Unit] =
    statusStream =>
      KafkaProducer
        .stream(producerSettings)
        .flatMap(producer =>
          statusStream.evalMap { case CommittableConsumerRecord(record, offset) =>
            producer
              .produceOne(ProducerRecord(outputTopic, record.key, record.value))
              .flatten
              .as(offset)
          }
        )
        .groupWithin(2, 10.seconds)
        .evalTapChunk(chunk => CommittableOffsetBatch.fromFoldable(chunk).commit)
        .void

  def stateFlow(state: BankState): Pipe[
    IO,
    CommittableConsumerRecord[IO, String, Either[DeserializationError, AccountOperationV2]],
    Unit
  ] = collectValidValues andThen updateBankState(state) andThen produceStatus

  val printFailures: Pipe[
    IO,
    CommittableConsumerRecord[IO, String, DeserializationError],
    Unit
  ] = _.evalMap(r => IO.println(r.toString)).void

  val failureFlow: Pipe[
    IO,
    CommittableConsumerRecord[IO, String, Either[DeserializationError, AccountOperationV2]],
    Unit
  ] = collectFailures andThen printFailures

  def process: fs2.Stream[IO, Unit] =
    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo(inputTopic)
      .records
      .broadcastThrough(stateFlow(state), failureFlow)
}

object BankOperationProgram {

  case class InvalidJson(rawBody: String, cause: ParsingFailure) extends DeserializationError {
    override def toString: String =
      s"Invalid-json, [$rawBody] is not a json, cause: ${cause.getMessage}"
  }

  case class InvalidEntity(jsonBody: Json, cause: DecodingFailure) extends DeserializationError {
    override def toString: String =
      s"Invalid-entity, [${jsonBody.noSpaces}], cause: ${cause.getMessage()}]"
  }

  case class UnexpectedError(cause: Throwable) extends DeserializationError
  implicit def deserializer[Body: Decoder]: Deserializer[IO, Either[DeserializationError, Body]] =
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
}
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
