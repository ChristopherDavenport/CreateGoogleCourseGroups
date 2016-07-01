package edu.eckerd.scripts.google

import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import edu.eckerd.google.api.services.directory.Directory
import edu.eckerd.scripts.google.persistence.GoogleTables
import edu.eckerd.scripts.google.methods.CreateGoogleCourseGroupsMethods
import slick.backend.DatabaseConfig
import slick.driver.JdbcProfile

import concurrent.ExecutionContext.Implicits.global
import concurrent.{Await, Future}
import concurrent.duration._
/**
  * Created by davenpcm on 6/30/16.
  */
object CreateGoogleCourseGroups extends CreateGoogleCourseGroupsMethods with GoogleTables with App {
  implicit val dbConfig: DatabaseConfig[JdbcProfile] = DatabaseConfig.forConfig("oracle")
  implicit val profile = dbConfig.driver
  implicit val db = dbConfig.db
  implicit val directory = Directory()

  val creating = CreateGoogleCourseGroups()

  val result = Await.result(creating, Duration.Inf)

  result.foreach { group =>
    println()
    group.foreach(println)
  }

}
