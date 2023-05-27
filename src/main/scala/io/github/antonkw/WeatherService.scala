package io.github.antonkw

import fs2.Pipe

trait WeatherService[F[_]] {
  def fahrenheitToCelsius: Pipe[F, Double, Double]
}

object WeatherService {
  def make[F[_]]: WeatherService[F] = new WeatherService[F] {
    override val fahrenheitToCelsius: Pipe[F, Double, Double] =
      _.map(fahrenheit => (fahrenheit - 32) * 5 / 9)
  }
}
