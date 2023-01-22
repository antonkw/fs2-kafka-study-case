package io.github.antonkw

import cats.effect.Outcome

import java.time.Instant
import scala.util.control.NoStackTrace

package object consumer {
  type EventTimestamp = Instant

  trait EventMeta {
    def eventType: String
    def correlationId: String
    def eventTimestamp: EventTimestamp
  }

  case class Error(message: String) extends NoStackTrace

  final case class Process[F[_]](outcome: F[Outcome[F, Throwable, Unit]], shutdown: F[Unit])
}
