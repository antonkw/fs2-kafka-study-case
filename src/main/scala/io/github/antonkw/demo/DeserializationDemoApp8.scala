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

import scala.concurrent.duration.DurationInt

sealed trait BankError extends NoStackTrace

case class AccountNotFound(accountId: UUID) extends BankError

case class InsufficientAmount(accountId: UUID, actual: Long) extends BankError

sealed trait AccountOperationResult {
  def accountId: UUID
  def sequenceId: Long
}
case class AccountUpdated(accountId: UUID, amount: Long, sequenceId: Long)
    extends AccountOperationResult
case class OperationIgnored(accountId: UUID, sequenceId: Long, actualSequence: Long)
    extends AccountOperationResult

trait BankState {
  def withdraw(
      withdrawal: WithdrawalOperationV2
  ): IO[Either[BankError, AccountOperationResult]]

  def replenish(replenishment: ReplenishmentOperationV2): IO[AccountOperationResult]
}

sealed trait AccountOperationV2 {
  def account: UUID
  def sequenceId: Long
}

object AccountOperationV2 extends AutoDerivation {
  implicit val accountOperationDecoder: Decoder[AccountOperationV2] =
    JsonTaggedAdtCodec.createDecoder("operation_type")

  implicit val accountOperationEncoder: Encoder[AccountOperationV2] =
    JsonTaggedAdtCodec.createEncoder("operation_type")
}

@JsonAdt("withdrawal:v2")
final case class WithdrawalOperationV2(account: UUID, value: Long, sequenceId: Long)
    extends AccountOperationV2

@JsonAdt("replenishment:v2")
final case class ReplenishmentOperationV2(account: UUID, value: Long, sequenceId: Long)
    extends AccountOperationV2

case class AccountState(amount: Long, latestSequenceId: Long)

object BankState {
  def make: IO[BankState] = IO.ref[Map[UUID, AccountState]](Map.empty).map { ref =>
    new BankState {
      def withdraw(
          withdrawal: WithdrawalOperationV2
      ): IO[Either[BankError, AccountOperationResult]] =
        ref.modify(state =>
          state.get(withdrawal.account) match {
            case Some(accountState) =>
              if (accountState.latestSequenceId >= withdrawal.sequenceId)
                (
                  state,
                  OperationIgnored(
                    withdrawal.account,
                    withdrawal.sequenceId,
                    accountState.latestSequenceId
                  ).asRight
                )
              else if (accountState.amount >= withdrawal.value)
                (
                  state.updated(
                    withdrawal.account,
                    AccountState(
                      amount = accountState.amount - withdrawal.value,
                      latestSequenceId = withdrawal.sequenceId
                    )
                  ),
                  AccountUpdated(
                    withdrawal.account,
                    accountState.amount - withdrawal.value,
                    withdrawal.sequenceId
                  ).asRight
                )
              else
                state -> InsufficientAmount(withdrawal.account, withdrawal.value).asLeft
            case None => state -> AccountNotFound(withdrawal.account).asLeft
          }
        )

      def replenish(replenishment: ReplenishmentOperationV2): IO[AccountOperationResult] =
        ref.modify { state =>
          state.get(replenishment.account) match {
            case Some(accountState) =>
              if (accountState.latestSequenceId >= replenishment.sequenceId)
                (
                  state,
                  OperationIgnored(
                    replenishment.account,
                    replenishment.sequenceId,
                    accountState.latestSequenceId
                  )
                )
              else {
                (
                  state.updated(
                    replenishment.account,
                    AccountState(
                      amount = accountState.amount + replenishment.value,
                      latestSequenceId = replenishment.sequenceId
                    )
                  ),
                  AccountUpdated(
                    replenishment.account,
                    accountState.amount + replenishment.value,
                    replenishment.sequenceId
                  )
                )
              }
            case None =>
              (
                state.updated(
                  replenishment.account,
                  AccountState(
                    amount = replenishment.value,
                    latestSequenceId = replenishment.sequenceId
                  )
                ),
                AccountUpdated(
                  replenishment.account,
                  replenishment.value,
                  replenishment.sequenceId
                )
              )
          }
        }
    }
  }
}

object DeserializationDemoApp8 extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
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
                  |  "operation_type" : "replenishment:v2" ,
                  |  "account": "123e4567-e89b-12d3-a456-426614174000",
                  |  "sequence_id" : 1,
                  |  "value": 200
                  |}
                  |""".stripMargin
              ),
              ProducerRecord(
                "topic1",
                "key",
                """
                  |{
                  |  "operation_type" : "replenishment:v2" ,
                  |  "account": "123e4567-e89b-12d3-a456-426614174000",
                  |  "sequence_id" : 1,
                  |  "value": 200
                  |}
                  |""".stripMargin
              )
            )
          )
        ).flatten
      )

    def program(state: BankState): fs2.Stream[IO, Unit] =
      KafkaConsumer
        .stream(consumerSettings)
        .subscribeTo("topic1")
        .records
        .evalTap { committable =>
          processRecord(state)(committable.record)
        }
        .groupWithin(2, 10.seconds)
        .evalTapChunk(chunk => CommittableOffsetBatch.fromFoldable(chunk.map(_.offset)).commit)
        .void

    produce *> fs2.Stream.eval(BankState.make).flatMap(program).compile.drain.as(ExitCode.Success)
  }
}
