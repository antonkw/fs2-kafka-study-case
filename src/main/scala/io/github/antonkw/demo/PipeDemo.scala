package io.github.antonkw.demo

import cats.effect.IO
import cats.effect.IOApp.Simple
import cats.implicits.catsSyntaxEitherId
import fs2.Pipe

object PipeDemo extends Simple {
  val inputStream: fs2.Stream[IO, Either[Int, String]] =
    fs2.Stream.emits(List(1.asLeft, "one".asRight))
  override def run: IO[Unit] = {
    val collectRights: Pipe[IO, Either[Int, String], String] =
      _.collect { case Right(value) => value }

    val collectLefts: Pipe[IO, Either[Int, String], String] =
      _.collect { case Left(value) => value.toString }

    inputStream.broadcastThrough(collectLefts, collectRights).map(IO.println).compile.drain
  }
}
