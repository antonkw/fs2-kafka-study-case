import sbt._

object Dependencies {
  private object Versions {
    val fs2Kafka = "3.0.0-M9"
  }

  val fs2Kafka = "com.github.fd4s" %% "fs2-kafka" % Versions.fs2Kafka
}