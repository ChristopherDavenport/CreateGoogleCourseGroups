package edu.eckerd.scripts.google.methods

import com.typesafe.scalalogging.LazyLogging
import edu.eckerd.google.api.services.directory.Directory
import edu.eckerd.scripts.google.persistence.GoogleTables._
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
/**
  * Created by davenpcm on 7/22/16.
  */
trait DeleteMembersNoLongerInCourseMethods extends LazyLogging {
  val profile : slick.driver.JdbcProfile
  val directory : Directory
  import profile.api._

  /**
    * This is the primary exposure point and what is run. It Deletes the Members from google, logs any errors that
    * were encountered on the delete and then delete all the successfully deleted records from the database.
    * @param db The database
    * @param ec The execution context
    * @return A Set of the Number Of Rows Effected By the Delete
    */
  def DeleteAllInactiveIndividuals()
                                  (implicit db: Database, ec: ExecutionContext): Future[Seq[Int]] = {
    val f = deleteDroppedMembersFromGoogle()
    f.map(_._2.foreach(bad => logger.error(s"Record Failed Deletion - $bad")))
    f.flatMap(t => deleteNonExistentMembersFromTable(t._1))
  }

  case class SimpleMembership(groupEmail: String, userEmail: String)
  private implicit val getClassGroupMemberResult = GetResult(r => SimpleMembership(r.<<, r.<<))

  /**
    * This is the statement that retrieves which terms we are processing for.
    * @param db The database to retrieve information from
    * @param ec The execution context
    * @return A Sequence of Strings that correspond to the terms.
    */
  private def getTerms()(implicit db: Database, ec: ExecutionContext): Future[Seq[String]] = {
    val data = sql"""SELECT DISTINCT GTVSDAX_EXTERNAL_CODE FROM GTVSDAX WHERE GTVSDAX_INTERNAL_CODE = 'ECTERM'
AND GTVSDAX_INTERNAL_CODE_GROUP IN ('ALIAS_UP', 'ALIAS_UP_XCRS', 'ALIAS_UR', 'ALIAS_UR_XCRS')""".as[String]

    db.run(data)
  }


  /**
    * Retrieves all Members of Groups Recorded in the Google Tables
    * @param currentTerms The terms we are doing processing for
    * @param db The database to retrieve from
    * @param ec The execution context
    * @return A Set of All Memberships currently contained in the google tables.
    */
  private def RetrieveAllMembers(currentTerms: Seq[String])
                                (implicit db: Database, ec: ExecutionContext): Future[Set[SimpleMembership]] = {

    val allAutoCourseGroups = for {
      groups <- googleGroups
      if groups.autoIndicator === "Y" && groups.autoType === "COURSE" && groups.autoTermCode.inSet(currentTerms)
    } yield groups

    val allAutoMembers = for {
      member <- googleGroupToUser if member.autoIndicator === "Y" && member.memberRole === "MEMBER"
    } yield member

    val allValidMembers = for {
      (group, member) <- allAutoCourseGroups join allAutoMembers on (_.id === _.groupId) if member.userEmail.isDefined
    } yield (group.email, member.userEmail.get)

    val fGroups = db.run(allValidMembers.result)

    fGroups.map(
      _.map {
        case (g, m) => SimpleMembership(g, m)
      }.toSet
    )
  }

  /**
    * This query returns all the members that should be created via the current banner statement. The email of the
    * group and the user email.
    * @param db The database to run against
    * @param ec The execution context
    * @return A future set of SimpleMembership containing group email and user email.
    */
  private def getTermGroupInformationFromDatabase()(implicit db: Database, ec: ExecutionContext)
  : Future[Set[SimpleMembership]] = {
    val data = sql"""SELECT
  lower(SSBSECT_SUBJ_CODE || SSBSECT_CRSE_NUMB || '-' || TO_CHAR(TO_NUMBER(SSBSECT_SEQ_NUMB)) ||
  '-' || decode(substr(SFRSTCR_TERM_CODE, -2, 1), 1, 'fa', 2, 'sp', 3, 'su') || '@eckerd.edu') as ALIAS,
  studentemail.GOREMAL_EMAIL_ADDRESS as STUDENT_EMAIL
FROM
  SFRSTCR
  INNER JOIN
  SSBSECT
  INNER JOIN SCBCRSE x
    ON SCBCRSE_CRSE_NUMB = SSBSECT_CRSE_NUMB
       AND SCBCRSE_SUBJ_CODE = SSBSECT_SUBJ_CODE
    ON SSBSECT_TERM_CODE = SFRSTCR_TERM_CODE
       AND SSBSECT_CRN = SFRSTCR_CRN
       AND SSBSECT_ENRL > 0
       AND REGEXP_LIKE(SSBSECT_SEQ_NUMB, '^-?\d+?$$')
  LEFT JOIN SIRASGN
    ON SIRASGN_TERM_CODE = SFRSTCR_TERM_CODE
       AND SIRASGN_CRN = SFRSTCR_CRN
       AND SIRASGN_PRIMARY_IND = 'Y'
       AND SIRASGN_PIDM is not NULL
  INNER JOIN STVRSTS
    ON STVRSTS_CODE = SFRSTCR_RSTS_CODE
       AND STVRSTS_INCL_SECT_ENRL = 'Y'
       AND STVRSTS_WITHDRAW_IND = 'N'
  INNER JOIN GOREMAL studentemail
    ON SFRSTCR_PIDM = GOREMAL_PIDM
    AND studentemail.GOREMAL_EMAL_CODE = 'CAS'
  INNER JOIN GOREMAL professorEmail
    ON SIRASGN_PIDM = professorEmail.GOREMAL_PIDM
    AND professorEmail.GOREMAL_EMAL_CODE = 'CA'
  INNER JOIN (
    SELECT DISTINCT GTVSDAX_EXTERNAL_CODE
    FROM GTVSDAX
    WHERE
      GTVSDAX_INTERNAL_CODE = 'ECTERM'
      AND GTVSDAX_INTERNAL_CODE_GROUP IN ('ALIAS_UP', 'ALIAS_UP_XCRS', 'ALIAS_UR', 'ALIAS_UR_XCRS')
    ) ON SFRSTCR_TERM_CODE = GTVSDAX_EXTERNAL_CODE
WHERE
  (SELECT MAX(SCBCRSE_EFF_TERM)
   FROM SCBCRSE y
   WHERE y.SCBCRSE_CRSE_NUMB = x.SCBCRSE_CRSE_NUMB
         AND y.SCBCRSE_SUBJ_CODE = x.SCBCRSE_SUBJ_CODE
  ) = x.SCBCRSE_EFF_TERM
ORDER BY alias asc""".as[SimpleMembership]

    db.run(data).map(_.toSet)
  }

  /**
    * This function generates the disjunction of the googleTables Membership and the Membership returned from
    * the Banner Query
    * @param db The database
    * @param ec The execution context
    * @return A set of simple Membership corresponding only to the Members that exist in the tables but are
    *         no longer returned by the fetch query.
    */
  private def generateDisjunction()(implicit db: Database, ec: ExecutionContext): Future[Set[SimpleMembership]] = {
    val fGoogleTable = getTerms().flatMap(RetrieveAllMembers)
    val fBanner = getTermGroupInformationFromDatabase()

    for {
      googleMembership <- fGoogleTable
      bannerMembership <- fBanner
    } yield googleMembership diff bannerMembership
  }

  /**
    * This is the function for deleting. It is more complicated as it is hard to delete when the original query spans
    * 2 tables and delete statements must be placed on a single table. So we use the information from the original
    * query to generate a query to get the primary key from the members table to delete the member
    * @param member The member to delete
    * @param db The database to get information from
    * @param ec The execution context
    * @return The Rows Deleted by the query. Dear god this sequence should only ever be length 1.
    */
  private def deleteMemberFromTable(member: SimpleMembership)(implicit db: Database, ec: ExecutionContext): Future[Seq[Int]] = {
    val g = for {
      group <- googleGroups if group.email === member.groupEmail
    } yield group

    val m = for {
      user <- googleGroupToUser if user.userEmail === member.userEmail
    } yield user

    val rowId = for {
      row <- g join m on (_.id === _.groupId)
    } yield (row._2.groupId, row._2.userID)

    db.run(rowId.result)
      .flatMap{
        Future.traverse(_){
          r =>
            deleteGoogleGroupRow(r._1, r._2)
        }
      }
  }

  /**
    * Simple Function for Deleting Based On the Primary Key of the Members Table
    * @param groupId The group Id
    * @param userId The user ID
    * @param db The database the information is from
    * @param ec The execution context
    * @return An Int corresponding to the rows deleted.
    */
  private def deleteGoogleGroupRow(groupId: String, userId: String)
                          (implicit db: Database, ec: ExecutionContext): Future[Int] = {
    val q = for {
      result <- googleGroupToUser if result.groupId === groupId && result.userID === userId
    } yield result

    db.run(q.delete)
  }

  /**
    * This calls the delete Members over any members in a set passed to it. This should be the set of all
    * values that was successfull deleted from Google.
    * @param set The set of Members to delete
    * @param db The database
    * @param ec The execution Context
    * @return A Sequence of How Many Rows Were Deleted
    */
  private def deleteNonExistentMembersFromTable(set: Set[SimpleMembership])
                                       (implicit db: Database, ec: ExecutionContext): Future[Seq[Int]] = {
    Future.sequence(set.toSeq.map(deleteMemberFromTable)).map(_.flatten)
  }

  /**
    * This function attempts to delete all the disjunct records from google. It then partitions the records depending
    * on which were successfully deleted from google and which where not.
    * @param db The database
    * @param ec The execution context
    * @return A tuple where on the right are the records that were successfully deleted and on the left are the records
    *         that failed to be deleted.
    */
  private def deleteDroppedMembersFromGoogle()
                                    (implicit db: Database, ec: ExecutionContext)
  : Future[(Set[SimpleMembership], Set[SimpleMembership])] = {
    val deletedFromGoogle = generateDisjunction()
      .map{ _.map {
        case SimpleMembership(groupEmail, userEmail) =>
          (SimpleMembership(groupEmail, userEmail), Try(directory.members.remove(groupEmail, userEmail)))
      }
      }

    val partitioned = deletedFromGoogle.map(_.partition( t => t._2.isSuccess))

    partitioned.map{
      p => (p._1.map(_._1), p._2.map(_._1))
    }
  }

}
