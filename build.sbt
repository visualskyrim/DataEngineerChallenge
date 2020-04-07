
lazy val versions = new {
  val sessionize = "1.1.0"
  val jodaTime = "2.9.3"
  val log4j = "1.2.17"
  val scalatest = "3.0.3"
  val sparkVersion = "2.3.3"
  val typesafe = "1.3.1"
}

lazy val root = (project in file("."))
  .configs(Test)
  .settings(
    inThisBuild(List(
      organization := "com.rakuten.rat",
      scalaVersion := "2.11.8",
      version := versions.sessionize
    )),
    name := "sessionize",
    fork := true,
    parallelExecution := false,
    libraryDependencies ++= Seq(
      "org.apache.thrift" % "libthrift" % "0.11.0",
      "org.apache.spark" %% "spark-core" % versions.sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % versions.sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % versions.sparkVersion % "provided",
      "com.typesafe" % "config" % versions.typesafe,
      "joda-time" % "joda-time" % versions.jodaTime,
      "log4j" % "log4j" % versions.log4j,
      "log4j" % "apache-log4j-extras" % versions.log4j,
      "com.sksamuel.elastic4s" %% "elastic4s-core" % "5.6.0",
      "com.sksamuel.elastic4s" %% "elastic4s-http" % "5.6.0",
      "org.json4s" %% "json4s-native" % "3.2.11",
      "org.json4s" %% "json4s-jackson" % "3.2.11",
      "org.scalatest" %% "scalatest" % versions.scalatest % "test",
      "com.github.scopt" %% "scopt" % "3.6.0"
    )
  )


parallelExecution in Test := false
parallelExecution := false

//unmanagedSourceDirectories in Compile += baseDirectory.value / "src" / "main" / "thrift-java"
//scroogeThriftSourceFolder in Compile := { baseDirectory.value / "whitelist-store-service/src/main/thrift" }

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

/* scalastyle >= 0.9.0 */
compileScalastyle := scalastyle.in(Compile).toTask("").value

lazy val upgrade = TaskKey[Unit]("upgrade", "Upgrade version")

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value
