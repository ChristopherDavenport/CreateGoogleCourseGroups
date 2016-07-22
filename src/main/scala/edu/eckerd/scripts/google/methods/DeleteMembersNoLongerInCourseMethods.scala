package edu.eckerd.scripts.google.methods

import edu.eckerd.google.api.services.directory.Directory
import edu.eckerd.scripts.google.persistence.GoogleTables._
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
/**
  * Created by davenpcm on 7/22/16.
  */
trait DeleteMembersNoLongerInCourseMethods {
  val profile : slick.driver.JdbcProfile
  val directory : Directory
  import profile.api._

  def DeleteAllInactiveIndividuals()
                                  (implicit db: Database, ec: ExecutionContext): Future[Seq[Int]] = {
    val f = deleteDroppedMembersFromGoogle()
    f.flatMap(t => deleteNonExistentMembersFromTable(t._1))

  }

  case class SimpleMembership(groupEmail: String, userEmail: String)
  private implicit val getClassGroupMemberResult = GetResult(r => SimpleMembership(r.<<, r.<<))

  private def getTerms()(implicit db: Database, ec: ExecutionContext): Future[Seq[String]] = {
    val data = sql"""SELECT DISTINCT GTVSDAX_EXTERNAL_CODE FROM GTVSDAX WHERE GTVSDAX_INTERNAL_CODE = 'ECTERM'
AND GTVSDAX_INTERNAL_CODE_GROUP IN ('ALIAS_UP', 'ALIAS_UP_XCRS', 'ALIAS_UR', 'ALIAS_UR_XCRS')""".as[String]

    db.run(data)
  }


  private def RetrieveAllMembers(currentTerms: Seq[String])(implicit db: Database, ec: ExecutionContext): Future[Set[SimpleMembership]] = {

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

  private def getTermGroupInformationFromDatabase()(implicit db: Database, ec: ExecutionContext): Future[Set[SimpleMembership]] = {
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


  private def generateDisjunction()(implicit db: Database, ec: ExecutionContext): Future[Set[SimpleMembership]] = {
    val fGoogleTable = getTerms().flatMap(RetrieveAllMembers)
    val fBanner = getTermGroupInformationFromDatabase()

    for {
      googleMembership <- fGoogleTable
      bannerMembership <- fBanner
    } yield googleMembership diff bannerMembership
  }

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

  private def deleteGoogleGroupRow(groupId: String, userId: String)
                          (implicit db: Database, ec: ExecutionContext): Future[Int] = {
    val q = for {
      result <- googleGroupToUser if result.groupId === groupId && result.userID === userId
    } yield result

    db.run(q.delete)
  }



  private def deleteNonExistentMembersFromTable(set: Set[SimpleMembership])
                                       (implicit db: Database, ec: ExecutionContext): Future[Seq[Int]] = {
    Future.sequence(set.toSeq.map(deleteMemberFromTable)).map(_.flatten)
  }

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
