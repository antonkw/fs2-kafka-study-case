ThisBuild / scalaVersion := "2.13.6"
ThisBuild / version      := "0.1.0"

ThisBuild / organization := "io.github.antonkw"
ThisBuild / organizationName := "antonkw"

def sonatypeS01Repo(status: String) =
  MavenRepository("sonatype-s01-" + status, "https://s01.oss.sonatype.org/content/repositories" + "/" + status)

ThisBuild / resolvers += sonatypeS01Repo("snapshots")

Compile / run / fork := true

testFrameworks += new TestFramework("weaver.framework.CatsEffect")

lazy val root = (project in file("."))
  .settings(
    name := "fs2kafka-demo",
    libraryDependencies ++= List(
      Dependencies.fs2Kafka,
      Dependencies.log4catsSlf4j,
      Dependencies.circeCore,
      Dependencies.circeParser,
      Dependencies.circeAdt,
      Dependencies.log4catsCore,
      Dependencies.logback,
      Dependencies.scalatest,
      Dependencies.weaverCats,
      Dependencies.weaverScalacheck
    )
  )
