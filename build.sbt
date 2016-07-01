enablePlugins(JavaAppPackaging)
name := "CreateGoogleCourseGroups"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("snapshots")

val slickV = "3.1.0"

libraryDependencies ++= List(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "edu.eckerd" %% "google-api-scala" % "0.0.1-SNAPSHOT",
  "com.typesafe.slick" %% "slick" % slickV,
  "com.typesafe.slick" %% "slick-extensions" % slickV ,
  "com.typesafe.slick" %% "slick-hikaricp" % slickV,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3"
)

unmanagedBase := baseDirectory.value / ".lib"

mainClass in Compile := Some("edu.eckerd.scripts.google.CreateGoogleCourseGroups")

mappings in Universal += {
  sourceDirectory.value / "main" / "resources" / "application.conf" -> "conf/application.conf"
}