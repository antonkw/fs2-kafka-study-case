import sbt._

object Dependencies {
  def typelevel(artifact: String, version: String) = "org.typelevel" %% artifact % version

  private object Versions {
    val fs2Kafka = "3.0.0-M9"
    val log4cats = "2.5.0"
    val logback  = "1.4.5"
    val circe    = "0.14.3"
  }

  val fs2Kafka      = "com.github.fd4s" %% "fs2-kafka"    % Versions.fs2Kafka
  val log4catsSlf4j = typelevel("log4cats-slf4j", Versions.log4cats)
  val log4catsCore  = typelevel("log4cats-core", Versions.log4cats)
  val circeCore     = "io.circe"        %% "circe-core"   % "0.14.3"
  val circeParser   = "io.circe"        %% "circe-parser" % "0.14.3"
  // Runtime
  val logback = "ch.qos.logback" % "logback-classic" % Versions.logback
}
