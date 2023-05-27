package io.github.antonkw.demo
//
//import cats.effect.{ExitCode, IO, IOApp, Ref}
//import cats.implicits.{catsSyntaxEitherId, catsSyntaxOptionId, toFunctorOps}
//import fs2.Pipe
//import fs2.kafka.KafkaProducer.pipe
//import fs2.kafka.{ConsumerRecord, ProducerRecords, _}
//import io.circe._
//import io.circe.generic.extras.AutoDerivation
//import io.github.antonkw.circe.adt.codec.JsonAdt
//
//import java.util.UUID
//import scala.util.control.NoStackTrace
//import io.github.antonkw.circe.adt.codec._
//import io.circe.syntax.EncoderOps
//
//import scala.concurrent.duration.DurationInt
//import scala.util.Either
//import scala.util.chaining.scalaUtilChainingOps
//
//object DeserializationDemoApp9 extends IOApp {
//  override def run(args: List[String]): IO[ExitCode] = {
//    def processRecord(bankState: BankState)(
//      record: ConsumerRecord[String, Either[Throwable, AccountOperationV2]]
//    ): IO[Unit] =
//      record.value match {
//        case Left(value) => IO.println(value)
//        case Right(operation) =>
//          operation match {
//            case op: WithdrawalOperationV2 => bankState.withdraw(op).flatMap(IO.println)
//            case op: ReplenishmentOperationV2 => bankState.replenish(op).flatMap(IO.println)
//          }
//      }
//
//    type Input = AccountOperationV2
//
//
//    def deserializer[Body: Decoder]: Deserializer[IO, Either[DeserializationError, Body]] =
//      Deserializer
//        .string[IO]
//        .flatMap(rawBody =>
//          io.circe.parser
//            .parse(rawBody)
//            .fold(
//              error => GenericDeserializer.fail[IO, Json](InvalidJson(rawBody, error)),
//              GenericDeserializer.const[IO, Json]
//            )
//        )
//        .flatMap(json =>
//          json
//            .as[Body]
//            .fold(
//              decodingFailure =>
//                GenericDeserializer
//                  .fail[IO, Body](InvalidEntity(json, decodingFailure)),
//              GenericDeserializer.const[IO, Body]
//            )
//        )
//        .attempt
//        .map(_.left.map {
//          case expected: DeserializationError => expected
//          case unexpected => UnexpectedError(unexpected)
//        })
//
//    implicit val accountOperationDeserializer
//    : Deserializer[IO, Either[DeserializationError, AccountOperationV2]] =
//      deserializer[AccountOperationV2]
//
//    val consumerSettings =
//      ConsumerSettings[IO, String, Either[DeserializationError, Input]]
//        .withAutoOffsetReset(AutoOffsetReset.Earliest)
//        .withBootstrapServers("localhost:29092")
//        .withGroupId("group")
//
//    val producerSettings =
//      ProducerSettings[IO, String, String]
//        .withBootstrapServers("localhost:29092")
//        .withProperty("topic.creation.enable", "true")
//
//    val produce = KafkaProducer
//      .resource(producerSettings)
//      .use(
//        _.produce(
//          ProducerRecords(
//            List(
//              ProducerRecord(
//                "topic1",
//                "key",
//                """
//                  |{
//                  |  "operation_type" : "replenishment:v2" ,
//                  |  "account": "123e4567-e89b-12d3-a456-426614174000",
//                  |  "sequence_id" : 1,
//                  |  "value": 200
//                  |}
//                  |""".stripMargin
//              ),
//              ProducerRecord(
//                "topic1",
//                "key",
//                """
//                  |{
//                  |  "operation_type" : "replenishment:v2" ,
//                  |  "account": "123e4567-e89b-12d3-a456-426614174000",
//                  |  "sequence_id" : 1,
//                  |  "value": 200
//                  |}
//                  |""".stripMargin
//              )
//            )
//          )
//        ).flatten
//      )
//
//    val collectValidValues: Pipe[
//      IO,
//      CommittableConsumerRecord[IO, String, Either[DeserializationError, Input]],
//      CommittableConsumerRecord[IO, String, Input]
//    ] = _.fproduct(_.record.value).collect { case (committable, Right(validValue)) =>
//      committable.as(validValue)
//    }
//
//    val collectFailures: Pipe[
//      IO,
//      CommittableConsumerRecord[IO, String, Either[DeserializationError, Input]],
//      CommittableConsumerRecord[IO, String, DeserializationError]
//    ] = _.fproduct(_.record.value).collect { case (committable, Left(failure)) =>
//      committable.as(failure)
//    }
//
//
//    def updateBankState(state: BankState): Pipe[
//      IO,
//      CommittableConsumerRecord[IO, String, Input],
//      CommittableConsumerRecord[IO, String, String]
//    ] = _.evalMap { case committable@CommittableConsumerRecord(record, _) =>
//      val resultIO: IO[Either[Throwable, Either[BankError, AccountOperationResult]]] = record.value match {
//        case withdrawal: WithdrawalOperationV2 =>
//          state.withdraw(withdrawal).attempt
//        case replenishment: ReplenishmentOperationV2 =>
//          state.replenish(replenishment).map(_.asRight[BankError]).attempt
//      }
//
//      val message: IO[String] = resultIO.map {
//        case Left(exception) => s"exception ${exception.getMessage}"
//        case Right(operationResult) => operationResult match {
//          case Left(bankError: BankError) => bankError match {
//            case AccountNotFound(accountId) => s"not found $accountId"
//            case InsufficientAmount(accountId, actual) => s"$accountId: lack of money $actual"
//          }
//          case Right(value) => value match {
//            case AccountUpdated(accountId, amount, sequenceId) =>
//              s"$accountId updated, amount $amount, sequence $sequenceId"
//            case OperationIgnored(accountId, sequenceId, actualSequence) =>
//              s"$accountId was not updated, sequence $sequenceId, actual sequence $actualSequence"
//          }
//        }
//      }
//
//      message.map(committable.as)
//    }
//
//    val produceStatus: Pipe[IO, CommittableConsumerRecord[IO, String, String], Unit] =
//      statusStream =>
//        KafkaProducer.stream(producerSettings)
//          .flatMap(producer => statusStream.evalMap {
//            case CommittableConsumerRecord(record, offset) =>
//              producer
//                .produceOne(ProducerRecord("operations", record.key, record.value))
//                .flatten
//                .as(offset)
//          })
//          .groupWithin(2, 10.seconds)
//          .evalTapChunk(chunk => CommittableOffsetBatch.fromFoldable(chunk).commit)
//          .void
//
//
//    def stateFlow(state: BankState): Pipe[
//      IO,
//      CommittableConsumerRecord[IO, String, Either[DeserializationError, Input]],
//      Unit
//    ] = collectValidValues andThen updateBankState(state) andThen produceStatus
//
//
//    val printFailures: Pipe[
//      IO,
//      CommittableConsumerRecord[IO, String, DeserializationError],
//      Unit
//    ] = _.evalMap(r => IO.println(r.toString)).void
//
//    val failureFlow: Pipe[
//      IO,
//      CommittableConsumerRecord[IO, String, Either[DeserializationError, Input]],
//      Unit
//    ] = collectFailures andThen printFailures
//
//    def program(state: BankState): fs2.Stream[IO, Unit] =
//      KafkaConsumer
//        .stream(consumerSettings)
//        .subscribeTo("topic1")
//        .records
//        .broadcastThrough(stateFlow(state), failureFlow)
//
//
//    produce *> fs2.Stream.eval(BankState.make).flatMap(program).compile.drain.as(ExitCode.Success)
//  }
//
//}
