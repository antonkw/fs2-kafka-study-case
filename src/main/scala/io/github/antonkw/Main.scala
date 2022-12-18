package io.github.antonkw

import cats.effect.{ExitCode, IO, IOApp}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = IO.delay(ExitCode.Success)
}
