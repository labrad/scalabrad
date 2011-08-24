name := "Scalabrad"

version := "0.1"

organization := "org.labrad"

scalaVersion := "2.9.0-1"

libraryDependencies += "junit" % "junit" % "4.8" % "test"

// netty
resolvers += "JBoss Netty repository" at "https://repository.jboss.org/nexus/content/repositories/releases/"

libraryDependencies += "org.jboss.netty" % "netty" % "3.2.4.Final"


// akka
resolvers += "Akka Repo" at "http://akka.io/repository"

// don't know why it can't find this automatically...
resolvers += "GuiceyFruit Release Repository" at "http://guiceyfruit.googlecode.com/svn/repo/releases/"

libraryDependencies += "se.scalablesolutions.akka" % "akka-typed-actor" % "1.1.2"


// logging
libraryDependencies += "org.clapper" % "grizzled-slf4j_2.9.0" % "0.5"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "0.9.29"


// scala test
//libraryDependencies += "org.scalatest" %% "scalatest" % "1.6.1" % "test"
libraryDependencies += "org.scalatest" % "scalatest_2.9.0" % "1.6.1" //% "test"


// specs2
resolvers += "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"

//libraryDependencies += "org.specs2" %% "specs2" % "1.4" % "test"

//libraryDependencies += "org.specs2" %% "specs2-scalaz-core" % "6.0.RC2" % "test"


// scala check
libraryDependencies += "org.scala-tools.testing" %% "scalacheck" % "1.9" //% "test"
