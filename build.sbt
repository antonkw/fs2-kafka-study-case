ThisBuild / scalaVersion := "2.13.6"
ThisBuild / version := "0.1.0"

ThisBuild / organization := "io.github.antonkw"
ThisBuild / organizationName := "antonkw"

Compile / run / fork := true

lazy val root = (project in file("."))
  .settings(
    name := "fs2kafka-demo",
    libraryDependencies ++= List(Dependencies.fs2Kafka, Dependencies.log4catsSlf4j, Dependencies.circeCore, Dependencies.circeParser, Dependencies.log4catsCore, Dependencies.logback)
  )