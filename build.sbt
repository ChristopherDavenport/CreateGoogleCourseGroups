enablePlugins(JavaAppPackaging)
name := "CreateGoogleCourseGroups"
version := "0.1.0"
maintainer := "Christopher Davenport <ChristopherDavenport@outlook.com>"
packageSummary := "This creates the groups for Eckerd College Moving Banner Courses into individual groups in Google"


scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("snapshots")
resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/"

val slickV = "3.1.0"

libraryDependencies ++= List(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "edu.eckerd" %% "google-api-scala" % "0.1.0",
  "com.typesafe.slick" %% "slick" % slickV,
  "com.typesafe.slick" %% "slick-extensions" % slickV ,
  "com.typesafe.slick" %% "slick-hikaricp" % slickV,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0",
  "ch.qos.logback" % "logback-classic" % "1.1.3"
)

unmanagedBase := baseDirectory.value / ".lib"

mainClass in Compile := Some("edu.eckerd.scripts.google.CreateGoogleCourseGroups")

mappings in Universal ++= Seq(
  sourceDirectory.value / "main" / "resources" / "application.conf" -> "conf/application.conf",
  sourceDirectory.value / "main" / "resources" / "logback.xml" -> "conf/logback.xml"
)

rpmVendor := "Eckerd College"
rpmLicense := Some("Apache 2.0")