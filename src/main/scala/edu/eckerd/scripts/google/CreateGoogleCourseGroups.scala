package edu.eckerd.scripts.google

import com.typesafe.scalalogging.LazyLogging
import edu.eckerd.google.api.services.directory.Directory
import edu.eckerd.scripts.google.persistence.GoogleTables
import edu.eckerd.scripts.google.methods.{CreateGoogleCourseGroupsMethods, DeleteMembersNoLongerInCourseMethods}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
/**
  * Created by davenpcm on 6/30/16.
  */
object CreateGoogleCourseGroups extends CreateGoogleCourseGroupsMethods with DeleteMembersNoLongerInCourseMethods
  with GoogleTables with App with LazyLogging {
  logger.info("Starting Create Google Groups Process")
  implicit val dbConfig: DatabaseConfig[JdbcProfile] = DatabaseConfig.forConfig("oracle")
  implicit val profile = dbConfig.profile
  implicit val db = dbConfig.db
  implicit val directory = Directory()

  val creating = CreateGoogleCourseGroups()

  val result = Await.result(creating, Duration(3, HOURS))

  result.filter(_._3 > 0).foreach(r => logger.info(s"$r"))

  val deletingOld = DeleteAllInactiveIndividuals()

  val result2 = Await.result(deletingOld, Duration(3, HOURS))

  logger.info(s"Deletion Process Removed - ${result2.sum}")
  logger.info("Exiting Create Google Groups Process Normally")
}
