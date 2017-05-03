lazy val commonSettings = Seq(
  organization := "org.labrad",

  version := {
    IO.read(file("core/src/main/resources/org/labrad/version.txt")).trim()
  },

  scalaVersion := "2.11.7",
  javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),

  scalacOptions ++= Seq(
    "-deprecation",
    "-feature"
  ),

  EclipseKeys.withSource := true,
  EclipseKeys.eclipseOutput := Some("target/eclipseOutput"),

  licenses += ("GPL-2.0", url("http://www.gnu.org/licenses/gpl-2.0.html")),

  // dependencies
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "com.lihaoyi" %% "fastparse" % "0.3.5",
    "org.clapper" %% "argot" % "1.0.4",
    "io.netty" % "netty-all" % "4.1.1.Final",
    "joda-time" % "joda-time" % "2.1",
    "org.joda" % "joda-convert" % "1.2",
    "org.slf4j" % "slf4j-api" % "1.7.2",
    "ch.qos.logback" % "logback-classic" % "1.0.6",
    "com.typesafe.play" %% "anorm" % "2.4.0-M2",
    "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
    "org.bouncycastle" % "bcprov-jdk15on" % "1.52",
    "org.bouncycastle" % "bcpkix-jdk15on" % "1.52",
    "org.mindrot" % "jbcrypt" % "0.3m",
    "com.google.api-client" % "google-api-client" % "1.19.0",
    "com.google.http-client" % "google-http-client" % "1.19.0"
  ),

  // When running, connect std in and tell manager to stop on EOF (ctrl+D).
  // This allows us to stop the manager without using ctrl+C, which kills sbt.
  fork in run := true,
  connectInput in run := true,
  javaOptions += "-Dorg.labrad.stopOnEOF=true",

  // testing
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.6" % "test"
  ),

  fork in Test := true,
  parallelExecution in Test := false,
  javaOptions in Test += "-Xmx1g",

  // use bintray to publish library jars
  bintrayOrganization := Some("labrad"),
  bintrayReleaseOnPublish in ThisBuild := false
)

lazy val all = project.in(file("."))
  .aggregate(core, manager)

lazy val core = project.in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "scalabrad-core"
  )

lazy val manager = project.in(file("manager"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "scalabrad-manager",

    packSettings,
    packMain := Map(
      "labrad" -> "org.labrad.manager.Manager",
      "labrad-migrate-registry" -> "org.labrad.registry.Migrate",
      "labrad-sql-test" -> "org.labrad.registry.SQLTest"
    ),
    packGenerateWindowsBatFile := true,
    packArchivePrefix := "scalabrad"
  )
