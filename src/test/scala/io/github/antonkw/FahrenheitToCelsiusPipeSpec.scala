package io.github.antonkw

import fs2._
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FahrenheitToCelsiusPipeSpec extends AnyFunSuite with Matchers {

  // The fahrenheitToCelsiusPipe to be tested
  test(
    "The fahrenheitToCelsius should convert the input temperatures from Fahrenheit to Celsius"
  ) {
    val weatherService = WeatherService.make[IO]

    // Define your test input and expected output
    val testInput: Stream[IO, Double] = Stream.emits[IO, Double](List(32.0, 212.0))
    val expectedOutput: List[Double] = List(0.0, 100.0)

    // Apply the pipe to the input stream and compile the stream into a list
    val actualOutput: IO[List[Double]] =
      testInput.through(weatherService.fahrenheitToCelsius).compile.toList

    // Compare the expected and actual output
    actualOutput.unsafeRunSync() shouldEqual expectedOutput
  }
}
