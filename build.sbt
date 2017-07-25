name := "CreateGoogleCourseGroups"
version := "0.1.0"

scalaVersion := "2.12.2"

resolvers += Resolver.sonatypeRepo("snapshots")
resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/"

val slickV = "3.2.1"

libraryDependencies ++= List(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "edu.eckerd" %% "google-api-scala" % "0.1.1",
  "com.typesafe.slick" %% "slick" % slickV,
  "com.typesafe.slick" %% "slick-hikaricp" % slickV,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

unmanagedBase := baseDirectory.value / ".lib"


